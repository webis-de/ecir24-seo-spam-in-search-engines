# Copyright 2021 Matti Wiegmann, Webis
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import click

from pathlib import Path
from transformers import RobertaForSequenceClassification, RobertaTokenizer
from datasets import load_dataset
import json
from datetime import datetime as dt
import torch
import logging


class Detector(object):
    """ Spin up OpenAIs GPT-detector model. We used the large version
    Azure DL: wget https://openaipublic.azureedge.net/gpt-2/detector-models/v1/detector-large.pt
    Github repo: https://github.com/openai/gpt-2-output-dataset

     We use the model here to qualify website text content quality after
     - (Bahri et al.) Generative Models are Unsupervised Predictors of Page Quality: A Colossal-Scale Study
     """
    def __init__(self, checkpoint: Path, device=-1):
        self.device = device
        self.device_name = "cpu" if device < 0 else f"cuda:{device}"
        self.model, self.tokenizer = self._load_model(checkpoint)

    def _load_model(self, checkpoint):
        """ Helper to load the model. Called on initialization of `Detector`

            Returns a tuple (RobertaForSequenceClassification, RobertaTokenizer)
        """
        data = torch.load(checkpoint, map_location='cpu')
        model_name = 'roberta-large' if data['args']['large'] else 'roberta-base'
        model = RobertaForSequenceClassification.from_pretrained(model_name, state_dict=data['model_state_dict'])
        model.to(self.device_name)
        tokenizer = RobertaTokenizer.from_pretrained(model_name)
        return model, tokenizer

    def detect(self, query: str):
        """ Make a detection for a single string-query.

            Returns a dict {used_tokens, real_probability, fake_probability)
         """
        inputs = self.tokenizer(
            query,
            padding=True,
            truncation=True,
            return_tensors="pt",
        ).to(self.device_name)

        with torch.no_grad():
            token_logits = self.model(**inputs).logits
            probs = token_logits.softmax(dim=-1)

        fake, real = probs.detach().cpu().flatten().numpy().tolist()

        return dict(used_tokens=self.tokenizer.batch_decode(inputs["input_ids"]),
                    real_probability=real,
                    fake_probability=fake)

    def annotate(self, dataset_file: Path):
        """ Load a Dataset and predict the gpt/original class for each example.
            The 'Original Text' Softmax prob is the annotated as `gpt_page_quality`.

            Returns a HF dataset with a field `gpt_page_quality` and all fields in `dataset_file`
        """
        def add_gpt_page_quality(examples):
            inputs = self.tokenizer(
                examples['main_content'],
                padding=True,
                truncation=True,
                return_tensors="pt",
            ).to(self.device_name)

            with torch.no_grad():
                outputs = self.model(**inputs).logits.softmax(dim=-1)

            ex = [x.detach().cpu().flatten().numpy().tolist()[1] for x in outputs]

            return {'gpt_page_quality': ex}

        dataset = load_dataset('json', data_files={"test": str(dataset_file)})
        dataset = dataset.map(add_gpt_page_quality, batched=True)

        return dataset['test']


@click.group()
def main():
    pass


@main.command()
@click.option('-c', '--checkpoint', type=click.Path(exists=True, dir_okay=False),
              help='the .pt dump of the model params')
@click.option('--device', type=click.INT,
              help='The device torch should use. Will be cpu (-1) if the given device is not available.',
              default=-1, show_default=True)
@click.option('-o', '--output_path', type=click.Path(), help='Path where the output will be written to.',
              default='./output/quality', show_default=True)
@click.argument('dataset_file')
def annotate(checkpoint: Path, device: int, output_path: Path, dataset_file: Path):
    """ This function adds two fields to the dataset:
        - generated: the probability that this is a generated text
        - original: the probability that this text is original and not generated

        Takes as argument a single jsonl file or a directory which contains many.
     """
    op = Path(output_path)
    op.mkdir(parents=True, exist_ok=True)
    if device > 0 and not torch.cuda.is_available():
        logging.warning("Given device is not available, falling back to 'CPU'")
        device = -1
    input_data_path = Path(dataset_file)
    if input_data_path.is_dir():
        input_files = list(input_data_path.glob("*.jsonl"))
    else:
        input_files = [input_data_path]

    d = Detector(checkpoint, device)
    for infile in input_files:
        result = d.annotate(infile)
        result.to_json(op / f"{infile.stem}.jsonl")


@main.command()
@click.option('-c', '--checkpoint', type=click.Path(exists=True, dir_okay=False),
              help='the .pt dump of the model params')
@click.option('-o', '--output_path', type=click.Path(file_okay=False),
              help='Path where the output will be written to.',
              default='./output/quality', show_default=True)
@click.option('--device', type=click.INT,
              help='The device torch should use. Will be cpu (-1) if the given device is not available.',
              default=-1, show_default=True)
@click.argument('q')
def gpt_detection(checkpoint: Path, output_path: Path, device: int, q: str):
    """ Return the predictions to a query and write it down """
    op = Path(output_path)
    op.mkdir(parents=True, exist_ok=True)

    if device > 0 and not torch.cuda.is_available():
        logging.warning("Given device is not available, falling back to 'CPU'")
        device = -1

    d = Detector(checkpoint, device)
    results = json.dumps(d.detect(q))
    logging.info(f"results: {results}")
    open(op / f"query-{dt.now()}.json").write(results)


if __name__ == "__main__":
    main()
