(() => {
  'use strict';
  const fallback = new URL(document.currentScript.dataset.fallback, document.baseURI).href;
  const isContentPhoto = (image) => image instanceof HTMLImageElement && image.closest('#main-content');
  const showFallback = (image) => {
    if (!isContentPhoto(image) || image.src === fallback) return;
    if (!image.dataset.originalPhotoAlt) image.dataset.originalPhotoAlt = image.alt;
    image.removeAttribute('srcset');
    image.removeAttribute('sizes');
    image.alt = 'Фото недоступне';
    image.src = fallback;
  };
  // Capture also covers lazy photos and later gallery changes; error does not bubble.
  document.addEventListener('error', (event) => showFallback(event.target), true);
  document.addEventListener('load', (event) => {
    const image = event.target;
    if (isContentPhoto(image) && image.src !== fallback && image.dataset.originalPhotoAlt !== undefined) {
      image.alt = image.dataset.originalPhotoAlt;
      delete image.dataset.originalPhotoAlt;
    }
  }, true);
  // An image can fail before this deferred script runs, including from browser cache.
  document.querySelectorAll('#main-content img').forEach((image) => {
    if (image.complete && image.naturalWidth === 0 && image.getAttribute('src')) showFallback(image);
  });
})();
