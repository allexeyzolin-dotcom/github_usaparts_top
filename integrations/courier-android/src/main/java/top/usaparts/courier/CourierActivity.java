package top.usaparts.courier;

import android.Manifest;
import android.app.Activity;
import android.content.Intent;
import android.content.pm.PackageManager;
import android.net.Uri;
import android.os.Bundle;
import android.webkit.PermissionRequest;
import android.webkit.ValueCallback;
import android.webkit.WebChromeClient;
import android.webkit.WebResourceRequest;
import android.webkit.WebView;
import android.webkit.WebViewClient;

public final class CourierActivity extends Activity {
    private WebView web;
    private PermissionRequest cameraRequest;
    private ValueCallback<Uri[]> fileRequest;
    private static final String HOME = "https://usaparts.top/courier/";

    private boolean trusted(Uri uri) {
        return "https".equals(uri.getScheme()) && "usaparts.top".equals(uri.getHost()) && uri.getPort() == -1;
    }

    @Override public void onCreate(Bundle state) {
        super.onCreate(state);
        getWindow().setStatusBarColor(0xff14394d);
        web = new WebView(this);
        web.getSettings().setJavaScriptEnabled(true);
        web.getSettings().setAllowFileAccess(false);
        web.getSettings().setMediaPlaybackRequiresUserGesture(false);
        web.setWebViewClient(new WebViewClient() {
            @Override public boolean shouldOverrideUrlLoading(WebView view, WebResourceRequest request) {
                Uri uri = request.getUrl();
                if (trusted(uri) && uri.getPath() != null && uri.getPath().startsWith("/courier/")) return false;
                if (!request.isForMainFrame() || !request.hasGesture()) return true;
                if ("tel".equals(uri.getScheme())) launch(new Intent(Intent.ACTION_DIAL, uri));
                else if ("https".equals(uri.getScheme()) || "http".equals(uri.getScheme())) launch(new Intent(Intent.ACTION_VIEW, uri));
                return true;
            }
        });
        web.setWebChromeClient(new WebChromeClient() {
            @Override public void onPermissionRequest(PermissionRequest request) {
                if (!trusted(request.getOrigin()) || !java.util.Arrays.asList(request.getResources()).contains(PermissionRequest.RESOURCE_VIDEO_CAPTURE)) {
                    request.deny(); return;
                }
                if (checkSelfPermission(Manifest.permission.CAMERA) == PackageManager.PERMISSION_GRANTED) {
                    request.grant(new String[]{PermissionRequest.RESOURCE_VIDEO_CAPTURE});
                } else {
                    if (cameraRequest != null) cameraRequest.deny();
                    cameraRequest = request;
                    requestPermissions(new String[]{Manifest.permission.CAMERA}, 1);
                }
            }
            @Override public void onPermissionRequestCanceled(PermissionRequest request) {
                if (cameraRequest == request) cameraRequest = null;
            }
            @Override public boolean onShowFileChooser(WebView view, ValueCallback<Uri[]> callback, FileChooserParams params) {
                if (fileRequest != null) fileRequest.onReceiveValue(null);
                fileRequest = callback;
                Intent picker = new Intent(Intent.ACTION_OPEN_DOCUMENT).addCategory(Intent.CATEGORY_OPENABLE).setType("image/*");
                try { startActivityForResult(picker, 2); }
                catch (android.content.ActivityNotFoundException e) { fileRequest.onReceiveValue(null); fileRequest = null; }
                return true;
            }
        });
        setContentView(web);
        web.loadUrl(HOME);
    }

    private void launch(Intent intent) {
        try { startActivity(intent); } catch (android.content.ActivityNotFoundException ignored) {}
    }
    @Override public void onRequestPermissionsResult(int code, String[] permissions, int[] results) {
        super.onRequestPermissionsResult(code, permissions, results);
        if (code == 1 && cameraRequest != null) {
            if (results.length > 0 && results[0] == PackageManager.PERMISSION_GRANTED)
                cameraRequest.grant(new String[]{PermissionRequest.RESOURCE_VIDEO_CAPTURE});
            else cameraRequest.deny();
            cameraRequest = null;
        }
    }
    @Override protected void onActivityResult(int code, int result, Intent data) {
        super.onActivityResult(code, result, data);
        if (code == 2 && fileRequest != null) {
            Uri uri = result == RESULT_OK && data != null ? data.getData() : null;
            fileRequest.onReceiveValue(uri != null && "content".equals(uri.getScheme()) ? new Uri[]{uri} : null);
            fileRequest = null;
        }
    }
    @Override public void onBackPressed() {
        web.evaluateJavascript("(function(){var d=document.querySelector('dialog[open]');if(d){d.close();return true;}var b=document.getElementById('back');if(b && b.offsetParent){b.click();return true;}return false;})()",
            handled -> { if (!"true".equals(handled)) finish(); });
    }
}
