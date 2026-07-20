import Cocoa
import Darwin
import WebKit

private enum MonitorLevel {
    case starting
    case healthy
    case warning
    case offline
}

final class AppDelegate: NSObject, NSApplicationDelegate, NSWindowDelegate, WKNavigationDelegate {
    private var window: NSWindow!
    private var webView: WKWebView!
    private var backend: Process?
    private var errorPipe: Pipe?
    private var stderrTail = Data()
    private let stderrTailLimit = 16_384
    private let stderrQueue = DispatchQueue(label: "app.moonvest.stderr-tail")
    private var readyFile: URL?
    private var startupTimer: Timer?
    private var startupStarted = Date()
    private var interfaceLoaded = false
    private var shuttingDown = false
    private var backendBaseURL: URL?
    private var statusItem: NSStatusItem!
    private var monitorTimer: Timer?
    private var monitorRequestInFlight = false
    private var sseMenuItem: NSMenuItem!
    private var brokerMenuItem: NSMenuItem!
    private var executionMenuItem: NSMenuItem!
    private var eventMenuItem: NSMenuItem!

    func applicationDidFinishLaunching(_ notification: Notification) {
        NSApp.setActivationPolicy(.regular)
        if let iconURL = Bundle.main.url(forResource: "AppIcon", withExtension: "icns"),
           let icon = NSImage(contentsOf: iconURL) {
            NSApp.applicationIconImage = icon
        }
        configureMenus()
        configureStatusItem()
        createWindow()
        showLoadingPage()
        startBackend()
        NSApp.activate(ignoringOtherApps: true)
    }

    func applicationShouldTerminateAfterLastWindowClosed(_ sender: NSApplication) -> Bool {
        false
    }

    func applicationShouldHandleReopen(_ sender: NSApplication, hasVisibleWindows flag: Bool) -> Bool {
        if window.isMiniaturized {
            window.deminiaturize(nil)
        }
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
        return true
    }

    func applicationWillTerminate(_ notification: Notification) {
        stopBackend()
    }

    func windowShouldClose(_ sender: NSWindow) -> Bool {
        if shuttingDown {
            return true
        }
        sender.miniaturize(nil)
        return false
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

    private func configureStatusItem() {
        statusItem = NSStatusBar.system.statusItem(withLength: NSStatusItem.variableLength)
        if let button = statusItem.button {
            button.image = monitorIcon(level: .starting)
            button.imagePosition = .imageLeft
            button.title = " 启动中"
            button.toolTip = "Moonvest 正在启动"
        }

        let menu = NSMenu(title: "Moonvest 监控")
        let header = NSMenuItem(title: "Moonvest 状态监控", action: nil, keyEquivalent: "")
        header.isEnabled = false
        menu.addItem(header)
        menu.addItem(NSMenuItem.separator())

        sseMenuItem = disabledMenuItem("SSE：正在启动")
        brokerMenuItem = disabledMenuItem("券商：正在检测")
        executionMenuItem = disabledMenuItem("订单执行：关闭")
        eventMenuItem = disabledMenuItem("最近事件：暂无")
        menu.addItem(sseMenuItem)
        menu.addItem(brokerMenuItem)
        menu.addItem(executionMenuItem)
        menu.addItem(eventMenuItem)
        menu.addItem(NSMenuItem.separator())

        let behavior = disabledMenuItem("关闭窗口只会最小化，后台继续运行")
        menu.addItem(behavior)
        menu.addItem(NSMenuItem.separator())
        menu.addItem(withTitle: "打开 Moonvest", action: #selector(showMainWindow(_:)), keyEquivalent: "")
        menu.addItem(withTitle: "立即刷新状态", action: #selector(refreshMonitorFromMenu(_:)), keyEquivalent: "")
        menu.addItem(NSMenuItem.separator())
        menu.addItem(withTitle: "退出 Moonvest", action: #selector(quitMoonvest(_:)), keyEquivalent: "")

        for item in menu.items where item.action != nil {
            item.target = self
        }
        statusItem.menu = menu
    }

    private func disabledMenuItem(_ title: String) -> NSMenuItem {
        let item = NSMenuItem(title: title, action: nil, keyEquivalent: "")
        item.isEnabled = false
        return item
    }

    private func monitorIcon(level: MonitorLevel) -> NSImage {
        let image = NSImage(size: NSSize(width: 18, height: 18))
        image.lockFocus()
        let color: NSColor
        switch level {
        case .starting: color = .systemGray
        case .healthy: color = .systemGreen
        case .warning: color = .systemOrange
        case .offline: color = .systemRed
        }
        color.setStroke()
        let outer = NSBezierPath(ovalIn: NSRect(x: 2.5, y: 2.5, width: 13, height: 13))
        outer.lineWidth = 1.8
        outer.stroke()
        let inner = NSBezierPath(ovalIn: NSRect(x: 6, y: 6, width: 6, height: 6))
        inner.lineWidth = 1.8
        inner.stroke()
        image.unlockFocus()
        image.isTemplate = false
        return image
    }

    @objc private func showMainWindow(_ sender: Any?) {
        if window.isMiniaturized {
            window.deminiaturize(nil)
        }
        window.makeKeyAndOrderFront(nil)
        NSApp.activate(ignoringOtherApps: true)
    }

    @objc private func refreshMonitorFromMenu(_ sender: Any?) {
        refreshMonitor()
    }

    @objc private func quitMoonvest(_ sender: Any?) {
        NSApp.terminate(nil)
    }

    private func startMonitorPolling() {
        monitorTimer?.invalidate()
        refreshMonitor()
        monitorTimer = Timer.scheduledTimer(withTimeInterval: 5, repeats: true) { [weak self] _ in
            self?.refreshMonitor()
        }
        monitorTimer?.tolerance = 1
    }

    private func refreshMonitor() {
        guard !shuttingDown, !monitorRequestInFlight,
              let url = backendBaseURL?.appendingPathComponent("api/dashboard") else {
            return
        }
        monitorRequestInFlight = true
        var request = URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData, timeoutInterval: 4)
        request.setValue("application/json", forHTTPHeaderField: "Accept")
        URLSession.shared.dataTask(with: request) { [weak self] data, response, error in
            DispatchQueue.main.async {
                guard let self else { return }
                self.monitorRequestInFlight = false
                guard error == nil,
                      let http = response as? HTTPURLResponse,
                      http.statusCode == 200,
                      let data,
                      let root = try? JSONSerialization.jsonObject(with: data) as? [String: Any] else {
                    self.updateMonitorUnavailable(error?.localizedDescription ?? "本机服务无响应")
                    return
                }
                self.updateMonitor(root)
            }
        }.resume()
    }

    private func updateMonitor(_ dashboard: [String: Any]) {
        let moonvest = dashboard["moonvest"] as? [String: Any] ?? [:]
        let health = dashboard["health"] as? [String: Any] ?? [:]
        let engine = dashboard["engine"] as? [String: Any] ?? [:]
        let sseConnected = moonvest["connected"] as? Bool ?? false
        let brokerConnected = health["connected"] as? Bool ?? false
        let armed = engine["armed"] as? Bool ?? false
        let brokerName = health["broker_label"] as? String ?? "券商"
        let follow = (moonvest["follow"] as? [String] ?? []).joined(separator: ", ")
        let lastError = moonvest["last_error"] as? String ?? ""
        let lastEvent = formattedMonitorTime(moonvest["last_event_at"] as? String)

        let level: MonitorLevel = sseConnected && brokerConnected ? .healthy :
            (sseConnected || brokerConnected ? .warning : .offline)
        let title = level == .healthy ? (armed ? " 执行中" : " 监控中") : " 需检查"
        statusItem.button?.image = monitorIcon(level: level)
        statusItem.button?.title = title
        statusItem.button?.toolTip = "Moonvest：SSE \(sseConnected ? "在线" : "离线") · \(brokerName) \(brokerConnected ? "在线" : "离线") · 执行\(armed ? "开启" : "关闭")"

        sseMenuItem.title = sseConnected
            ? "SSE：在线\(follow.isEmpty ? "" : " · \(follow)")"
            : "SSE：离线\(lastError.isEmpty ? "" : " · \(lastError.prefix(72))")"
        brokerMenuItem.title = "券商：\(brokerName) · \(brokerConnected ? "在线" : "离线")"
        executionMenuItem.title = "订单执行：\(armed ? "已启用" : "已关闭")"
        eventMenuItem.title = "最近事件：\(lastEvent)"
    }

    private func updateMonitorUnavailable(_ reason: String) {
        statusItem.button?.image = monitorIcon(level: .offline)
        statusItem.button?.title = " 需检查"
        statusItem.button?.toolTip = "Moonvest 本机服务不可用：\(reason)"
        sseMenuItem.title = "SSE：本机服务不可用"
        brokerMenuItem.title = "券商：无法读取状态"
        executionMenuItem.title = "订单执行：状态未知"
        eventMenuItem.title = "错误：\(reason.prefix(80))"
    }

    private func formattedMonitorTime(_ value: String?) -> String {
        guard let value, !value.isEmpty else { return "暂无" }
        let iso = ISO8601DateFormatter()
        iso.formatOptions = [.withInternetDateTime, .withFractionalSeconds]
        let date = iso.date(from: value) ?? ISO8601DateFormatter().date(from: value)
        guard let date else { return String(value.prefix(19)) }
        let formatter = DateFormatter()
        formatter.locale = Locale(identifier: "zh_CN")
        formatter.dateFormat = "MM-dd HH:mm:ss"
        return formatter.string(from: date)
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
        // 必须持续排空 stderr：管道缓冲约 64KB，写满后后端会在写 stderr 时
        // 永久阻塞，表现为整个服务假死。只保留末尾片段用于错误展示。
        stderrQueue.sync { stderrTail.removeAll() }
        stderr.fileHandleForReading.readabilityHandler = { [weak self] handle in
            let chunk = handle.availableData
            guard let self, !chunk.isEmpty else { return }
            self.stderrQueue.async {
                self.stderrTail.append(chunk)
                if self.stderrTail.count > self.stderrTailLimit {
                    self.stderrTail.removeFirst(self.stderrTail.count - self.stderrTailLimit)
                }
            }
        }
        process.terminationHandler = { [weak self] finished in
            DispatchQueue.main.async {
                guard let self else { return }
                self.errorPipe?.fileHandleForReading.readabilityHandler = nil
                guard !self.shuttingDown else { return }
                let data = self.stderrQueue.sync { self.stderrTail }
                let message = String(data: data, encoding: .utf8)?.trimmingCharacters(in: .whitespacesAndNewlines)
                self.updateMonitorUnavailable(message?.isEmpty == false ? message! : "内置服务意外退出")
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
            backendBaseURL = url
            webView.load(URLRequest(url: url, cachePolicy: .reloadIgnoringLocalCacheData))
            startMonitorPolling()
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
        monitorTimer?.invalidate()
        errorPipe?.fileHandleForReading.readabilityHandler = nil
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
