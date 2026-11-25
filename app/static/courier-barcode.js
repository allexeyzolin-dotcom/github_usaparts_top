(function(root, factory) {
  const api = factory();
  if (typeof module === 'object' && module.exports) module.exports = api;
  else root.CourierBarcode = api;
})(typeof window === 'undefined' ? globalThis : window, function() {
  // ZXing's browser bundle exports BarcodeFormat, but not DecodeHintType.
  // TRY_HARDER = 3 in the bundled @zxing/library enum.
  const TRY_HARDER = 3;

  function regions(width, height, rgba) {
    const gray = new Uint8Array(width * height);
    for (let i = 0; i < gray.length; i++) {
      const p = i * 4;
      gray[i] = (rgba[p] * 77 + rgba[p + 1] * 150 + rgba[p + 2] * 29) >> 8;
    }
    const normal = locate(gray, width, height, false);
    const narrow = locate(gray, width, height, true);
    const result = [], seen = new Set();
    for (let i = 0; i < Math.max(normal.length, narrow.length); i++) {
      for (const region of [normal[i], narrow[i]]) {
        if (!region) continue;
        const key = [region.x, region.y, region.width, region.height].join(':');
        if (!seen.has(key)) {seen.add(key); result.push(region);}
      }
    }
    return result;
  }

  function locate(gray, width, height, narrow) {
    const edges = new Uint8Array(gray.length), rows = new Float64Array(height);
    for (let y = 2; y < height - 2; y++) {
      const start = y * width;
      for (let x = 2; x < width - 2; x++) {
        const p = start + x;
        // Multiple spans avoid aliasing on one- and two-pixel bars.
        const horizontal = narrow ? Math.max(Math.abs(gray[p + 1] - gray[p]),
          Math.abs(gray[p + 1] - gray[p - 1]), Math.abs(gray[p + 2] - gray[p - 2]))
          : Math.abs(gray[p + 2] - gray[p - 2]);
        const vertical = narrow ? Math.max(Math.abs(gray[p + width] - gray[p]),
          Math.abs(gray[p + width] - gray[p - width]), Math.abs(gray[p + width * 2] - gray[p - width * 2]))
          : Math.abs(gray[p + width * 2] - gray[p - width * 2]);
        const edge = Math.max(0, horizontal - vertical);
        edges[p] = edge;
        rows[y] += edge;
      }
    }
    const band = Math.max(16, Math.min(48, Math.round(width / 24)));
    const scores = new Float64Array(height);
    let sum = 0;
    for (let y = 0; y < height; y++) {
      sum += rows[y];
      if (y >= band) sum -= rows[y - band];
      if (y >= band - 1) scores[y - Math.floor(band / 2)] = sum;
    }
    const result = [];
    for (let n = 0; n < 10; n++) {
      let peak = 0;
      for (let y = 1; y < height; y++) if (scores[y] > scores[peak]) peak = y;
      if (!scores[peak]) break;
      const top = Math.max(2, peak - Math.floor(band / 2));
      const bottom = Math.min(height - 2, top + band);
      const columns = new Float64Array(width);
      for (let y = top; y < bottom; y++) {
        for (let x = 2; x < width - 2; x++) columns[x] += edges[y * width + x];
      }
      const smooth = new Float64Array(width);
      const radius = Math.max(3, Math.round(width / 120));
      let max = 0, rolling = 0;
      for (let x = 0; x < width; x++) {
        rolling += columns[x];
        if (x > radius * 2) rolling -= columns[x - radius * 2 - 1];
        if (x >= radius * 2) {
          smooth[x - radius] = rolling;
          max = Math.max(max, rolling);
        }
      }
      for (const level of [0.15, 0.3]) {
        let left = -1;
        for (let x = 0; x <= width; x++) {
          if (x < width && smooth[x] > max * level) { if (left < 0) left = x; }
          else if (left >= 0) {
            const span = x - left;
            if (span >= Math.max(50, band * 2) && span < width * 0.95) {
              const pad = Math.max(16, Math.round(span * 0.35));
              const x0 = Math.max(0, left - pad), x1 = Math.min(width, x + pad);
              const y0 = Math.max(0, top - 6), y1 = Math.min(height, bottom + 6);
              result.push({x:x0, y:y0, width:x1-x0, height:y1-y0});
            }
            left = -1;
          }
        }
      }
      for (let y = Math.max(0, peak - band); y < Math.min(height, peak + band); y++) scores[y] = 0;
    }
    return result;
  }

  function createReader(ZXing, makeCanvas = () => document.createElement('canvas')) {
    const hints = new Map([[TRY_HARDER, true]]);
    const reader = new ZXing.BrowserMultiFormatReader(hints, {
      delayBetweenScanAttempts: 220, delayBetweenScanSuccess: 250, tryPlayVideoTimeout: 8000
    });
    reader.possibleFormats = [ZXing.BarcodeFormat.CODE_128, ZXing.BarcodeFormat.ITF,
      ZXing.BarcodeFormat.CODE_39, ZXing.BarcodeFormat.QR_CODE];
    reader.live = false;
    let regionOffset = 0;
    const decode = reader.decodeFromCanvas.bind(reader);
    const analysis = makeCanvas(), crop = makeCanvas();
    reader.decodeFromCanvas = function(canvas) {
      let originalError;
      try { return decode(canvas); } catch (error) { originalError = error; }
      if (canvas.width < 32 || canvas.height < 16) throw originalError;
      const ratio = Math.min(1, 960 / canvas.width);
      analysis.width = Math.round(canvas.width * ratio);
      analysis.height = Math.round(canvas.height * ratio);
      const context = analysis.getContext('2d', {willReadFrequently:true});
      context.drawImage(canvas, 0, 0, analysis.width, analysis.height);
      const pixels = context.getImageData(0, 0, analysis.width, analysis.height).data;
      const candidates = regions(analysis.width, analysis.height, pixels);
      const count = reader.live ? Math.min(8, candidates.length) : candidates.length;
      const start = reader.live ? regionOffset : 0;
      regionOffset = candidates.length ? (start + count) % candidates.length : 0;
      for (let i = 0; i < count; i++) {
        const region = candidates[(start + i) % candidates.length];
        // Isolate bars from the receipt, text and dark background before ZXing's
        // thresholding. Upscaling helps narrow bars photographed from a screen.
        const scale = Math.min(3, 1200 * ratio / region.width);
        crop.width = Math.round(region.width * scale / ratio);
        crop.height = Math.round(region.height * scale / ratio);
        const target = crop.getContext('2d');
        target.imageSmoothingEnabled = true;
        target.imageSmoothingQuality = 'high';
        target.drawImage(canvas, region.x / ratio, region.y / ratio, region.width / ratio, region.height / ratio,
          0, 0, crop.width, crop.height);
        try { return decode(crop); } catch (_) { /* Try the next barcode region. */ }
      }
      // Keep ZXing's NotFoundException type so its live scan loop continues.
      throw originalError;
    };
    return reader;
  }
  return {createReader, regions};
});
