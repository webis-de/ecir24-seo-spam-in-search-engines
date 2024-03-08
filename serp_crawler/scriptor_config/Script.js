const fs = require("fs-extra");
const path = require("path");

const { AbstractScriptorScript, files, pages, log } = require("@webis-de/scriptor");
const { PlaywrightBlocker } = require("@cliqz/adblocker-playwright");

const NAME = "Snapshot";
const VERSION = "0.3.0";

const waitForLoadStateWithTimeout = async (page, event, timeout) => {
  try {
    return await page.waitForLoadState(event, { timeout: timeout });
  } catch (ex) {
    return null;
  }
};

const waitForNavigationWithTimeout = async (page, waitUntil, timeout) => {
  try {
    return await page.waitForNavigation({ waitUntil: waitUntil, timeout: timeout });
  } catch (ex) {
    return null;
  }
};

const loadBlocker = () => {
  const path = '/workdir/playwright-adblocker.bin';
  if (fs.existsSync(path)) {
    return PlaywrightBlocker.deserialize(fs.readFileSync(path));
  }
  const b = PlaywrightBlocker.parse(fs.readFileSync('/script/blocklist.txt', 'utf-8'));
  fs.writeFileSync(path, b.serialize());
  return b;
};
const blocker = loadBlocker();

module.exports = class extends AbstractScriptorScript {

  constructor() {
    super(NAME, VERSION);
  }

  async run(browserContexts, scriptDirectory, inputDirectory, outputDirectory) {
    const browserContext = browserContexts[files.BROWSER_CONTEXT_DEFAULT];

    // Script options
    const defaultScriptOptions = {
      viewportAdjust: {},
      snapshot: {
        screenshot: { timeout: 120000 }  // Screenshotting complex pages can take a very long time
      }
    };
    const requiredScriptOptions = [ "url" ];
    const scriptOptions = files.readOptions(files.getExisting(
      files.SCRIPT_OPTIONS_FILE_NAME, [ scriptDirectory, inputDirectory ]),
      defaultScriptOptions, requiredScriptOptions);
    log.info({options: scriptOptions}, "script.options");

    fs.writeJsonSync(path.join(outputDirectory, files.SCRIPT_OPTIONS_FILE_NAME), scriptOptions);

    // Load page(s)
    let url = scriptOptions["url"];
    if (typeof url === "string") {
      url = [url];
    }

    const promises = [];
    for (const [i, u] of url.entries()) {
      const page = await browserContext.newPage();
      await blocker.enableBlockingInPage(page);
      const origViewport = await page.viewportSize();
      promises.push(page.goto(u, { waitUntil: "domcontentloaded" }).then(async (resp) => {
        // Wait for load and any potential navigation thereafter
        await waitForLoadStateWithTimeout(page, "load", 20000);
        await waitForNavigationWithTimeout(page, "load", 1000);

        // Adjust viewport height to scroll height to trigger loading dynamic content
        await pages.adjustViewportToPage(page, scriptOptions["viewportAdjust"]);

        // Wait for three networkidle intervals to ensure dynamic content finished loading
        for (let i = 0; i < 3; ++i) {
          await waitForLoadStateWithTimeout(page, "networkidle", 10000);
        }

        // Update viewport up to three times or until taller than 50k pixels to accomodate
        // for layout changes and to trigger further dynamic content
        for (let resizes = 0; resizes < 3; ++resizes) {
          const h = await page.viewportSize().height;
          if (h > 50000 || h === await pages.getHeight(page)) {
            // Viewport hasn't changed or already too tall
            break;
          }
          await pages.adjustViewportToPage(page, scriptOptions["viewportAdjust"]);
          await page.waitForTimeout(500);
          await waitForLoadStateWithTimeout(page, "networkidle", 6000);
          await waitForLoadStateWithTimeout(page, "networkidle", 6000);
        }

        // Reset to original width to avoid strange scaling effects on some pages
        await page.setViewportSize({width: origViewport.width, height: await pages.getHeight(page)});
        await page.waitForTimeout(500);
        await waitForLoadStateWithTimeout(page, "networkidle", 6000);

        // Take snapshot(s)
        const snapName = url.length > 1 ? `snapshot-${i}` : "snapshot";
        await pages.takeSnapshot(page, Object.assign(
            { path: path.join(outputDirectory, snapName) }, scriptOptions["snapshot"]
        ));
      }));
    }
    await Promise.all(promises);

    return true;
  }
};
