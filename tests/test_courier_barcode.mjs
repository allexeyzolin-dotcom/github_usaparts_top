import test from 'node:test';
import assert from 'node:assert/strict';
import {createRequire} from 'node:module';
const require = createRequire(import.meta.url);
const {regions, createReader} = require('../app/static/courier-barcode.js');

function image(width, height, bars) {
  const rgba = new Uint8Array(width * height * 4).fill(255);
  if (bars) {
    for (let y = bars.y; y < bars.y + bars.height; y++) {
      for (let x = bars.x; x < bars.x + bars.width; x++) {
        // Thin alternating bars with a low-contrast gray print on white paper.
        const gray = Math.floor((x - bars.x) / 2) % 2 ? 235 : 90;
        rgba.set([gray,gray,gray,255], (y * width + x) * 4);
      }
    }
  }
  return rgba;
}

test('locates small barcode regions above, below and away from the image center', () => {
  for (const bar of [
    {x:50,y:25,width:150,height:28},
    {x:390,y:200,width:150,height:28},
    {x:180,y:410,width:150,height:28}
  ]) {
    const found = regions(640,480,image(640,480,bar));
    assert.ok(found.some(r => r.x <= bar.x && r.x+r.width >= bar.x+bar.width &&
      r.y < bar.y+bar.height && r.y+r.height > bar.y), JSON.stringify({bar,found}));
    for (const region of found) {
      assert.ok(region.x >= 0 && region.y >= 0);
      assert.ok(region.x + region.width <= 640 && region.y + region.height <= 480);
    }
  }
});

test('blank images do not create barcode candidates', () => {
  assert.deepEqual(regions(640,480,image(640,480)), []);
});

test('a failed enhanced attempt preserves ZXing error identity for continuous camera retries', () => {
  const notFound = new Error('ZXing NotFoundException');
  class Reader {
    constructor(hints) { this.hints = hints; }
    decodeFromCanvas() { throw notFound; }
  }
  const pixels = image(80,40);
  const makeCanvas = () => ({width:1,height:1,getContext:() => ({drawImage(){},getImageData:() => ({data:pixels})})});
  const reader = createReader({BrowserMultiFormatReader:Reader,BarcodeFormat:{}},makeCanvas);
  assert.equal(reader.hints.get(3),true);
  assert.throws(() => reader.decodeFromCanvas({width:80,height:40}), error => error === notFound);
});

test('clear codes return immediately without extra image processing', () => {
  const result = {getText:() => '20450000001111'};
  class Reader { decodeFromCanvas() { return result; } }
  const reader = createReader({BrowserMultiFormatReader:Reader,BarcodeFormat:{}}, () => ({}));
  assert.equal(reader.decodeFromCanvas({width:640,height:480}),result);
});
