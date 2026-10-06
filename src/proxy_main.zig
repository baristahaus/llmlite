const std = @import("std");
const builtin = @import("builtin");
const proxy = @import("proxy");
const tui = @import("tui");
const hot_reload = @import("hot_reload");

pub fn main(init: std.process.Init) !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();
    const io = init.io;

    // Parse --tui flag, --port=<n> (default 4000), and --aliases=<path>
    var use_tui = false;
    var listen_port: u16 = 4000;
    var aliases_path: ?[]const u8 = null;
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, "--tui")) {
            use_tui = true;
        } else if (std.mem.startsWith(u8, arg, "--port=")) {
            listen_port = std.fmt.parseInt(u16, arg[7..], 10) catch 4000;
        } else if (std.mem.startsWith(u8, arg, "--aliases=")) {
            aliases_path = arg[10..];
        }
    }

    std.log.info("llmlite-proxy starting...", .{});

    // Initialize components
    var key_store = proxy.VirtualKeyStore.init(allocator);
    defer key_store.deinit();

    var rate_limiter = proxy.RateLimiter.init(allocator);
    defer rate_limiter.deinit();

    var request_logger = try proxy.RequestLogger.init(allocator, null, true);
    defer request_logger.deinit();

    var metrics = proxy.MetricsCollector{};

    // Add test API key
    try key_store.add("sk-test-key", .{
        .user_id = "test-user",
        .rate_limit = 100,
        .allowed_models = null,
    });

    std.log.info("Added test API key: sk-test-key", .{});
    std.log.info("Virtual Keys: enabled", .{});
    std.log.info("Multi-tenancy: disabled (set via config)", .{});
    std.log.info("Cost Tracking: disabled (set via config)", .{});
    std.log.info("Cache: disabled (set via config)", .{});
    std.log.info("KV Backend: memory (in-memory, zero dependency)", .{});

    const port: u16 = listen_port;

    // Edge config: env-var-driven base-URL overrides for local backends
    // (cross-test setup 2026-10-03). Slot mapping (all OpenAI-format passthrough):
    //   openai    <- LLMLITE_OPENAI_BASE_URL    (q38fn world A head, 172.30.30.25:8010)
    //   deepseek  <- LLMLITE_DEEPSEEK_BASE_URL  (q38fn world B head, 172.30.30.27:8010)
    //   moonshot  <- LLMLITE_MOONSHOT_BASE_URL  (glm53 world head,   172.30.30.29:8010)
    //   minimax   <- LLMLITE_MINIMAX_BASE_URL   (kyber 27b vLLM,     172.30.30.20:8000)
    // Per-slot API keys via OPENAI_API_KEY / DEEPSEEK_API_KEY / MOONSHOT_API_KEY / MINIMAX_API_KEY.
    // Unprefixed model requests route to the openai slot (deterministic; latency tracking off).
    var edge_config = hot_reload.getDefaultEdgeConfig();
    if (std.c.getenv("LLMLITE_OPENAI_BASE_URL")) |c| edge_config.openai_base_url = std.mem.span(c);
    if (std.c.getenv("LLMLITE_DEEPSEEK_BASE_URL")) |c| edge_config.deepseek_base_url = std.mem.span(c);
    if (std.c.getenv("LLMLITE_MOONSHOT_BASE_URL")) |c| edge_config.moonshot_base_url = std.mem.span(c);
    if (std.c.getenv("LLMLITE_MINIMAX_BASE_URL")) |c| edge_config.minimax_base_url = std.mem.span(c);
    edge_config.enabled_providers = "openai,deepseek,moonshot,minimax";
    edge_config.enable_latency_tracking = false;

    var server = try proxy.Server.initWithEdgeConfig(
        allocator,
        io,
        port,
        &key_store,
        &rate_limiter,
        &request_logger,
        &metrics,
        edge_config,
        aliases_path,
    );
    errdefer server.deinit();

    if (use_tui) {
        // Register SIGWINCH for terminal resize support
        if (builtin.os.tag == .linux or builtin.os.tag == .macos) {
            const sa = std.posix.Sigaction{
                .handler = .{ .handler = tui.handleSigwinch },
                .mask = std.posix.sigemptyset(),
                .flags = std.posix.SA.RESTART,
            };
            std.posix.sigaction(std.posix.SIG.WINCH, &sa, null);
        }

        // Server runs in background thread
        const ServerCtx = struct {
            s: *proxy.Server,
            fn run(ctx: @This()) void {
                ctx.s.start() catch |err| {
                    std.log.err("server error: {}", .{err});
                };
            }
        };
        const server_thread = try std.Thread.spawn(.{}, ServerCtx.run, .{ServerCtx{ .s = &server }});

        // Give server a moment to start listening
        std.Io.sleep(io, std.Io.Duration.fromMilliseconds(200), .real) catch {};

        // TUI runs in main thread
        var stdout_buffer: [4096]u8 = undefined;
        var stdout_writer = std.Io.File.Writer.init(.stdout(), io, &stdout_buffer);
        const stdout = &stdout_writer.interface;
        var tui_instance = tui.Tui.init(allocator, io);
        defer tui_instance.deinit(stdout);

        tui_instance.state.latency_tracker = &server.latency_tracker;
        tui_instance.state.health_checker = &server.health_checker;
        tui_instance.state.circuit_breaker = &server.circuit_breaker;
        tui_instance.state.metrics = &metrics;
        tui_instance.state.is_service_running = true;

        tui_instance.enableRawMode(stdout) catch {
            std.log.warn("Failed to enable raw mode, running without TUI", .{});
            server.stop();
            server_thread.join();
            return;
        };

        // Run the new event loop (poll-based, no busy-wait)
        tui.runEventLoop(&tui_instance.state, stdout) catch |err| {
            std.log.err("TUI error: {}", .{err});
        };

        server.stop();
        server_thread.join();
    } else {
        std.log.info("llmlite-proxy listening on http://0.0.0.0:{d}", .{port});
        try server.start();
    }
}
