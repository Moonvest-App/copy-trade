import Cocoa
import Darwin
import WebKit

final class AppDelegate: NSObject, NSApplicationDelegate, NSWindowDelegate, WKNavigationDelegate {
    private var window: NSWindow!
    private var webView: WKWebView!
    private var backend: Process?
    private var errorPipe: Pipe?
    private var readyFile: URL?
    private var startupTimer: Timer?
    private var startupStarted = Date()
    private var interfaceLoaded = false
    private var shuttingDown = false

    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.regular)
        if let iconURL = Bundle.main.url(forResource: "AppIcon", withExtension: "icns"),
           let icon = NSImage(contentsOf: iconURL) {
            NSApp.applicationIconImage = icon
        }
        configureMenus()
        createWindow()
        showLoadingPage()
        startBackend()
        NSApp.activate(ignoringOtherApps: true)
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        true
    }

    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows flag: Bool) -> Bool {
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
        return true
    }

    func applicationWillTerminate(_ notification: Notification) {
        stopBackend()
    }

    func windowWillClose(_ notification: Notification) {
        stopBackend()
    }

    private func configureMenus() {
        let mainMenu = NSMenu()
        let appMenuItem = NSMenuItem()
        mainMenu.addItem(appMenuItem)

        let appMenu = NSMenu()
        appMenu.addItem(withTitle: "关于 Moonvest", action: #selector(NSApplication.orderFrontStandardAboutPanel(_:)), keyEquivalent: "")
        appMenu.addItem(NSMenuItem.separator())
        appMenu.addItem(withTitle: "隐藏 Moonvest", action: #selector(NSApplication.hide(_:)), keyEquivalent: "h")
        appMenu.addItem(withTitle: "隐藏其他", action: #selector(NSApplication.hideOtherApplications(_:)), keyEquivalent: "h").keyEquivalentModifierMask = [.command, .option]
        appMenu.addItem(NSMenuItem.separator())
        appMenu.addItem(withTitle: "退出 Moonvest", action: #selector(NSApplication.terminate(_:)), keyEquivalent: "q")
        appMenuItem.submenu = appMenu

        let editMenuItem = NSMenuItem()
        mainMenu.addItem(editMenuItem)
        let editMenu = NSMenu(title: "编辑")
        let undoItem = editMenu.addItem(withTitle: "撤销", action: Selector(("undo:")), keyEquivalent: "z")
        undoItem.keyEquivalentModifierMask = [.command]
        let redoItem = editMenu.addItem(withTitle: "重做", action: Selector(("redo:")), keyEquivalent: "Z")
        redoItem.keyEquivalentModifierMask = [.command, .shift]
        editMenu.addItem(NSMenuItem.separator())
        let cutItem = editMenu.addItem(withTitle: "剪切", action: #selector(NSText.cut(_:)), keyEquivalent: "x")
        cutItem.keyEquivalentModifierMask = [.command]
        let copyItem = editMenu.addItem(withTitle: "复制", action: #selector(NSText.copy(_:)), keyEquivalent: "c")
        copyItem.keyEquivalentModifierMask = [.command]
        let pasteItem = editMenu.addItem(withTitle: "粘贴", action: #selector(NSText.paste(_:)), keyEquivalent: "v")
        pasteItem.keyEquivalentModifierMask = [.command]
        editMenu.addItem(NSMenuItem.separator())
        let selectAllItem = editMenu.addItem(withTitle: "全选", action: #selector(NSText.selectAll(_:)), keyEquivalent: "a")
        selectAllItem.keyEquivalentModifierMask = [.command]
        editMenuItem.submenu = editMenu

        let windowMenuItem = NSMenuItem()
        mainMenu.addItem(windowMenuItem)
        let windowMenu = NSMenu(title: "窗口")
        windowMenu.addItem(withTitle: "最小化", action: #selector(NSWindow.performMiniaturize(_:)), keyEquivalent: "m")
        windowMenu.addItem(withTitle: "缩放", action: #selector(NSWindow.performZoom(_:)), keyEquivalent: "")
        windowMenuItem.submenu = windowMenu
        NSApp.windowsMenu = windowMenu
        NSApp.mainMenu = mainMenu
    }

    private func createWindow() {
        let configuration = WKWebViewConfiguration()
        configuration.websiteDataStore = .default()
        webView = WKWebView(frame: .zero, configuration: configuration)
        webView.navigationDelegate = self
        webView.setValue(false, forKey: "drawsBackground")

        window = NSWindow(
            contentRect: NSRect(x: 0, y: 0, width: 1320, height: 860),
            styleMask: [.titled, .closable, .miniaturizable, .resizable, .fullSizeContentView],
            backing: .buffered,
            defer: false
        )
        window.title = "Moonvest"
        window.titlebarAppearsTransparent = true
        window.titleVisibility = .hidden
        window.backgroundColor = NSColor(
            calibratedRed: 7.0 / 255.0,
            green: 14.0 / 255.0,
            blue: 28.0 / 255.0,
            alpha: 1.0
        )
        window.isMovableByWindowBackground = true
        window.minSize = NSSize(width: 980, height: 680)
        window.contentView = webView
        window.delegate = self
        window.center()
        window.setFrameAutosaveName("MoonvestMainWindow")
        window.makeKeyAndOrderFront(nil)
    }

    private func showLoadingPage() {
        let html = """
        <!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
        <style>
        :root{color-scheme:dark}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 78% 8%,rgba(23,107,94,.18),transparent 32%),linear-gradient(145deg,#070e1c,#0a1524 56%,#0b1a28);color:#f5f7fb;font:15px -apple-system,BlinkMacSystemFont,"PingFang SC",sans-serif;display:grid;place-items:center;height:100vh}.card{text-align:center;padding:40px 48px;background:rgba(17,29,47,.9);border:1px solid rgba(148,163,184,.16);border-radius:22px;box-shadow:0 24px 70px rgba(0,0,0,.3)}.mark{width:46px;height:46px;margin:0 auto 18px;border:4px solid rgba(62,224,160,.16);border-top-color:#3ee0a0;border-radius:50%;animation:spin .9s linear infinite}h1{font-size:22px;font-weight:850;margin:0 0 9px}p{color:#8492a9;margin:0}@keyframes spin{to{transform:rotate(360deg)}}
        </style></head><body><div class="card"><div class="mark"></div><h1>正在启动 Moonvest</h1><p>正在启动本机服务并检查券商 API…</p></div></body></html>
        """
        webView.loadHTMLString(html, baseURL: nil)
    }

    private func showFailure(_ detail: String) {
        guard !shuttingDown else { return }
        startupTimer?.invalidate()
        let escaped = detail
            .replacingOccurrences(of: "&", with: "&amp;")
            .replacingOccurrences(of: "<", with: "&lt;")
            .replacingOccurrences(of: ">", with: "&gt;")
        let html = """
        <!doctype html><html lang="zh-CN"><head><meta charset="utf-8"><style>
        :root{color-scheme:dark}*{box-sizing:border-box}body{margin:0;background:radial-gradient(circle at 78% 8%,rgba(23,107,94,.18),transparent 32%),linear-gradient(145deg,#070e1c,#0a1524 56%,#0b1a28);color:#f5f7fb;font:15px -apple-system,BlinkMacSystemFont,"PingFang SC",sans-serif;display:grid;place-items:center;height:100vh}.card{max-width:620px;padding:38px 44px;background:rgba(17,29,47,.94);border:1px solid rgba(255,107,107,.22);border-radius:22px;box-shadow:0 24px 70px rgba(0,0,0,.32)}h1{font-size:22px;font-weight:850;margin:0 0 12px;color:#ff6b6b}p{line-height:1.6;color:#aab5c6;white-space:pre-wrap;margin:0}
        </style></head><body><div class="card"><h1>应用启动失败</h1><p>\(escaped)</p></div></body></html>
        """
        webView.loadHTMLString(html, baseURL: nil)
    }

    private func startBackend() {
        guard let resources = Bundle.main.resourceURL else {
            showFailure("找不到应用资源目录。")
            return
        }
        let executable = resources
            .appendingPathComponent("backend", isDirectory: true)
            .appendingPathComponent("Moonvest Backend")
        guard FileManager.default.isExecutableFile(atPath: executable.path) else {
            showFailure("找不到内置服务程序。请重新安装应用。")
            return
        }

        let ready = FileManager.default.temporaryDirectory
            .appendingPathComponent("moonvest-\(getpid()).port")
        try? FileManager.default.removeItem(at: ready)
        readyFile = ready

        let process = Process()
        let stderr = Pipe()
        process.executableURL = executable
        process.arguments = ["--no-browser", "--port", "8899", "--ready-file", ready.path]
        process.standardOutput = FileHandle.nullDevice
        process.standardError = stderr
        process.terminationHandler = { [weak self] finished in
            DispatchQueue.main.async {
                guard let self, !self.shuttingDown, !self.interfaceLoaded else { return }
                let data = self.errorPipe?.fileHandleForReading.availableData ?? Data()
                let message = String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines)
                self.showFailure(message?.isEmpty == false ? message! : "内置服务意外退出（状态码 \(finished.terminationStatus)）。")
            }
        }
        backend = process
        errorPipe = stderr
        startupStarted = Date()

        do {
            try process.run()
        } catch {
            showFailure("无法启动内置服务：\(error.localizedDescription)")
            return
        }

        startupTimer = Timer.scheduledTimer(withTimeInterval: 0.12, repeats: true) { [weak self] timer in
            self?.checkBackendReady(timer)
        }
    }

    private func checkBackendReady(_ timer: Timer) {
        guard let readyFile else { return }
        if let contents = try? String(contentsOf: readyFile, encoding: .utf8),
           let port = Int(contents.trimmingCharacters(in: .whitespacesAndNewlines)),
           let url = URL(string: "http://127.0.0.1:\(port)/") {
            timer.invalidate()
            interfaceLoaded = true
            webView.load(URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData))
            return
        }
        if Date().timeIntervalSince(startupStarted) > 35 {
            timer.invalidate()
            showFailure("内置服务启动超时。请重新打开 App；若仍失败，请查看本机日志。")
        }
    }

    private func stopBackend() {
        guard !shuttingDown else { return }
        shuttingDown = true
        startupTimer?.invalidate()
        if let process = backend, process.isRunning {
            process.terminate()
            let deadline = Date().addingTimeInterval(2)
            while process.isRunning && Date() < deadline {
                RunLoop.current.run(until: Date().addingTimeInterval(0.05))
            }
            if process.isRunning {
                kill(process.processIdentifier, SIGKILL)
            }
        }
        if let readyFile {
            try? FileManager.default.removeItem(at: readyFile)
        }
    }

    func webView(
        _ webView: WKWebView,
        decidePolicyFor navigationAction: WKNavigationAction,
        decisionHandler: @escaping (WKNavigationActionPolicy) -> Void
    ) {
        guard let url = navigationAction.request.url else {
            decisionHandler(.cancel)
            return
        }
        if url.scheme == "about" || (url.scheme == "http" && ["127.0.0.1", "localhost"].contains(url.host ?? "")) {
            decisionHandler(.allow)
        } else if url.scheme == "https" {
            NSWorkspace.shared.open(url)
            decisionHandler(.cancel)
        } else {
            decisionHandler(.cancel)
        }
    }
}

let application = NSApplication.shared
let delegate = AppDelegate()
application.delegate = delegate
application.run()
