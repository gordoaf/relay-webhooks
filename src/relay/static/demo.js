/* Standalone browser simulation. No network requests or real webhook delivery. */
(() => {
  if (!window.RELAY_DEMO) return;
  const now = () => Date.now() / 1000;
  const id = (prefix) =>
    prefix + "_" + crypto.randomUUID().replaceAll("-", "").slice(0, 24);
  let state;
  const secret = () =>
    "whsec_" +
    btoa(String.fromCharCode(...crypto.getRandomValues(new Uint8Array(32))));
  function endpoint(
    project,
    url,
    description = "Warehouse receiver",
    rateLimit = 10,
    subscriptions = ["*"],
  ) {
    const ep = {
      id: id("ep"),
      project_id: project,
      url,
      description,
      enabled: true,
      rate_limit: rateLimit,
      subscriptions,
      created_at: now(),
    };
    state.endpoints.push(ep);
    return { ...ep, signing_secret: secret() };
  }
  function event(project, type, payload) {
    const eventId = id("evt"),
      stamp = now();
    let count = 0;
    for (const ep of state.endpoints.filter(
      (e) =>
        e.project_id === project &&
        e.enabled &&
        e.subscriptions.some(
          (pattern) =>
            pattern === "*" ||
            pattern === type ||
            (pattern.endsWith(".*") && type.startsWith(pattern.slice(0, -1))),
        ),
    )) {
      state.deliveries.unshift({
        id: id("dlv"),
        project_id: project,
        event_id: eventId,
        event_type: type,
        payload,
        endpoint_id: ep.id,
        endpoint_url: ep.url,
        status: "pending",
        attempt_count: 0,
        cycle_attempts: 0,
        next_attempt_at: stamp + 0.8,
        created_at: stamp,
        updated_at: stamp,
        last_status_code: null,
        last_error: null,
        attempts: [],
      });
      count++;
    }
    state.deliveries = state.deliveries.slice(0, 200);
    return {
      event: { id: eventId, delivery_count: count, payload },
      created: true,
    };
  }
  function reset() {
    state = {
      projects: [{ id: "prj_demo", name: "Demo shop", created_at: now() }],
      endpoints: [],
      deliveries: [],
      recoveries: [],
      previews: [],
      online: true,
      received: 0,
    };
    endpoint(
      "prj_demo",
      "https://warehouse.example.com/webhooks",
      "Order fulfillment",
    );
    for (let i = 0; i < 8; i++) {
      event(
        "prj_demo",
        ["order.created", "shipment.updated", "order.created", "invoice.paid"][
          i % 4
        ],
        { order_id: "order_" + (1034 + i), amount: 2400, currency: "USD" },
      );
      const d = state.deliveries[0];
      d.created_at = now() - (8 - i) * 83;
      d.updated_at = d.created_at;
      d.status = i === 3 ? "failed" : "delivered";
      d.next_attempt_at = null;
      d.attempt_count = i === 3 ? 1 : i === 5 ? 2 : 1;
      d.cycle_attempts = d.attempt_count;
      d.last_status_code = i === 3 ? 422 : 204;
      d.last_error = i === 3 ? "HTTP 422" : null;
      for (let n = 1; n <= d.attempt_count; n++)
        d.attempts.push({
          attempt_number: n,
          status_code: n < d.attempt_count ? 503 : d.last_status_code,
          error: n < d.attempt_count ? "HTTP 503" : d.last_error,
          duration_ms: 24 + i * 9,
          response_excerpt: i === 3 ? "Missing required field: address" : "",
          created_at: d.created_at + n,
        });
    }
  }
  function tick() {
    for (const job of state.recoveries.filter((j) => j.status === "running")) {
      const count = job.queued && job.next_release_at <= now() ? 1 : 0;
      for (let i = 0; i < count; i++) {
        const deliveryId = job.members.shift();
        const d = state.deliveries.find((row) => row.id === deliveryId);
        job.queued--;
        if (d?.status === "failed") {
          d.status = "pending";
          d.cycle_attempts = 0;
          d.next_attempt_at = now();
          job.released.push(d.id);
          job.requeued++;
        } else job.skipped++;
        job.next_release_at = now() + 1 / job.pace_per_second;
      }
      if (!job.queued) job.status = "completed";
    }
    for (const d of state.deliveries) {
      if (
        !["pending", "retrying", "processing"].includes(d.status) ||
        d.next_attempt_at > now()
      )
        continue;
      if (!state.endpoints.find((e) => e.id === d.endpoint_id)?.enabled)
        continue;
      if (d.status !== "processing") {
        d.status = "processing";
        d.endpoint_url = state.endpoints.find(
          (e) => e.id === d.endpoint_id,
        ).url;
        d.next_attempt_at = now() + 0.5;
        continue;
      }
      d.attempt_count++;
      d.cycle_attempts++;
      d.updated_at = now();
      d.last_status_code = state.online ? 204 : 503;
      d.last_error = state.online ? null : "HTTP 503";
      d.attempts.push({
        attempt_number: d.attempt_count,
        status_code: d.last_status_code,
        error: d.last_error,
        duration_ms: 36,
        response_excerpt: state.online ? "" : "Warehouse receiver unavailable",
        endpoint_url: d.endpoint_url,
        created_at: now(),
      });
      d.status = state.online
        ? "delivered"
        : d.cycle_attempts >= 6
          ? "failed"
          : "retrying";
      d.next_attempt_at =
        d.status === "retrying"
          ? now() + Math.min(2 ** d.cycle_attempts, 30)
          : null;
      if (state.online) state.received++;
    }
  }
  function operations(project) {
    const rows = state.deliveries.filter(
      (d) => !project || d.project_id === project,
    );
    const pending = rows.filter((d) =>
      ["pending", "retrying", "processing"].includes(d.status),
    );
    const attempts = rows
      .flatMap((d) => d.attempts)
      .filter((a) => a.created_at > now() - 900);
    const failed = attempts.filter(
      (a) => a.status_code >= 300 || !a.status_code,
    );
    return {
      observed_at: now(),
      queue: {
        depth: pending.length,
        pending: pending.filter((d) => d.status === "pending").length,
        processing: pending.filter((d) => d.status === "processing").length,
        retrying: pending.filter((d) => d.status === "retrying").length,
        oldest_age_seconds: pending.length
          ? now() - Math.min(...pending.map((d) => d.created_at))
          : 0,
      },
      outcomes: {
        window_seconds: 900,
        attempts: attempts.length,
        failed_attempts: failed.length,
        failure_percent: attempts.length
          ? (failed.length / attempts.length) * 100
          : 0,
        avg_latency_ms: attempts.length
          ? attempts.reduce((s, a) => s + a.duration_ms, 0) / attempts.length
          : 0,
      },
      endpoints: { cooling_down: 0 },
      storage: {
        total_bytes: 2097152 + state.deliveries.length * 1024,
        free_bytes: 8 * 1024 ** 3,
      },
      policy: {
        retention_days: 30,
        idempotency_days: 90,
        max_storage_bytes: 0,
      },
      alerts:
        !state.online && pending.length
          ? [
              {
                code: "receiver_unavailable",
                active: true,
                message:
                  "The simulated receiver is offline. Bring it online to let queued deliveries recover.",
                updated_at: now(),
              },
            ]
          : [],
    };
  }
  async function request(path, method = "GET", body = {}) {
    tick();
    const url = new URL(path, location.origin),
      parts = url.pathname.split("/").filter(Boolean);
    if (path === "/api/session") return { ok: true, demo_enabled: true };
    if (path === "/api/operations") return operations();
    if (path === "/api/maintenance/cleanup")
      return {
        enabled: true,
        dry_run: body.dry_run,
        retention_days: 30,
        eligible_events: 0,
        deleted_events: 0,
        deleted_deliveries: 0,
        deleted_attempts: 0,
        has_more: false,
      };
    if (url.pathname === "/api/projects") {
      if (method === "GET") return { projects: state.projects };
      if (!body.name?.trim()) throw Error("Enter a project name.");
      const p = { id: id("prj"), name: body.name, created_at: now() };
      state.projects.push(p);
      return { ...p, api_key: "relay_demo_" + id("key") };
    }
    if (path === "/api/demo/receiver") {
      if (method === "POST") state.online = body.online;
      return { online: state.online, received: state.received };
    }
    if (path === "/api/demo/send")
      return event(body.project_id, "order.created", {
        order_id: "order_" + Math.floor(1000 + Math.random() * 9000),
        amount: 2400,
        currency: "USD",
      });
    const project = parts[2];
    if (!state.projects.some((p) => p.id === project))
      throw Error("Select a project first.");
    if (parts[3] === "rotate-key")
      return {
        api_key: "relay_demo_" + id("key"),
        previous_valid_until: now() + body.overlap_seconds,
      };
    if (parts[3] === "metrics") return operations(project);
    if (parts[3] === "recoveries") {
      if (parts[4] === "preview") {
        const matching = state.deliveries.filter(
          (d) =>
            d.project_id === project &&
            d.status === "failed" &&
            (!body.endpoint_id || d.endpoint_id === body.endpoint_id) &&
            (!body.since || d.updated_at >= body.since) &&
            (!body.until || d.updated_at <= body.until),
        );
        const selected = matching.slice(0, body.limit || 1000);
        const preview = {
          preview_id: id("pre"),
          project_id: project,
          total: matching.length,
          selected_count: selected.length,
          expires_at: now() + 900,
          members: selected.map((d) => d.id),
        };
        state.previews.push(preview);
        return structuredClone(preview);
      }
      if (method === "GET")
        return {
          recoveries: state.recoveries
            .filter((j) => j.project_id === project)
            .map((j) => {
              const outcomes = {
                pending: 0,
                processing: 0,
                retrying: 0,
                delivered: 0,
                failed: 0,
                removed: 0,
              };
              for (const deliveryId of j.released || []) {
                const row = state.deliveries.find((d) => d.id === deliveryId);
                outcomes[row?.status || "removed"]++;
              }
              return { ...structuredClone(j), outcomes };
            }),
        };
      if (parts[5] === "cancel") {
        const job = state.recoveries.find(
          (j) => j.id === parts[4] && j.project_id === project,
        );
        if (!job) throw Error("Recovery not found.");
        job.cancelled += job.queued;
        job.queued = 0;
        job.members = [];
        job.status = "cancelled";
        return structuredClone(job);
      }
      const preview = state.previews.find(
        (p) => p.preview_id === body.preview_id && p.project_id === project,
      );
      if (!preview || preview.expires_at < now())
        throw Error("Preview expired. Preview failures again.");
      const existing = state.recoveries.find(
        (j) => j.preview_id === body.preview_id,
      );
      if (existing) return structuredClone(existing);
      const job = {
        id: id("rec"),
        project_id: project,
        preview_id: preview.preview_id,
        status: preview.selected_count ? "running" : "completed",
        total: preview.selected_count,
        queued: preview.selected_count,
        requeued: 0,
        skipped: 0,
        cancelled: 0,
        pace_per_second: body.pace_per_second,
        next_release_at: now(),
        created_at: now(),
        members: [...preview.members],
        released: [],
      };
      state.recoveries.unshift(job);
      return structuredClone(job);
    }
    if (parts[3] === "endpoints") {
      if (method === "GET")
        return {
          endpoints: state.endpoints.filter((e) => e.project_id === project),
        };
      if (method === "POST" && parts.length === 4) {
        let parsed;
        try {
          parsed = new URL(body.url);
        } catch {
          throw Error("Enter a valid HTTP or HTTPS URL.");
        }
        if (!["http:", "https:"].includes(parsed.protocol))
          throw Error("Use HTTP or HTTPS.");
        return endpoint(
          project,
          body.url,
          body.description,
          Number(body.rate_limit) || 10,
          body.subscriptions || ["*"],
        );
      }
      const ep = state.endpoints.find(
        (e) => e.id === parts[4] && e.project_id === project,
      );
      if (!ep) throw Error("Endpoint not found.");
      if (parts[5] === "rotate-secret")
        return {
          signing_secret: secret(),
          previous_valid_until: now() + body.overlap_seconds,
        };
      Object.assign(ep, body);
      return ep;
    }
    if (parts[3] === "events") return event(project, body.type, body.payload);
    const rows = state.deliveries.filter((d) => d.project_id === project);
    if (parts[3] === "stats") {
      const s = {
        total: rows.length,
        pending: 0,
        retrying: 0,
        processing: 0,
        delivered: 0,
        failed: 0,
      };
      rows.forEach((d) => s[d.status]++);
      s.success_rate = s.total ? (s.delivered / s.total) * 100 : 0;
      return s;
    }
    if (parts[3] === "deliveries") {
      if (parts[4]) {
        const d = rows.find((d) => d.id === parts[4]);
        if (!d) throw Error("Delivery not found.");
        if (parts[5] === "replay") {
          if (d.status === "processing")
            throw Error("Delivery is currently processing");
          d.status = "pending";
          d.cycle_attempts = 0;
          d.next_attempt_at = now() + 0.8;
          return { ok: true };
        }
        return structuredClone(d);
      }
      const status = url.searchParams.get("status"),
        search = (url.searchParams.get("search") || "").toLowerCase();
      const filtered = rows.filter(
        (d) =>
          (!status || d.status === status) &&
          [d.event_id, d.event_type, d.endpoint_url].some((s) =>
            s.toLowerCase().includes(search),
          ),
      );
      const offset = Number(url.searchParams.get("offset") || 0),
        limit = Number(url.searchParams.get("limit") || 12);
      return {
        items: structuredClone(filtered.slice(offset, offset + limit)),
        total: filtered.length,
      };
    }
    throw Error("This operation is not available in the browser demo.");
  }
  reset();
  setInterval(tick, 250);
  window.relayDemo = { request, reset };
})();
