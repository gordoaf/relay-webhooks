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
  ) {
    const ep = {
      id: id("ep"),
      project_id: project,
      url,
      description,
      enabled: true,
      rate_limit: rateLimit,
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
      (e) => e.project_id === project && e.enabled,
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
  async function request(path, method = "GET", body = {}) {
    tick();
    const url = new URL(path, location.origin),
      parts = url.pathname.split("/").filter(Boolean);
    if (path === "/api/session") return { ok: true, demo_enabled: true };
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
    if (parts[3] === "endpoints") {
      if (method === "GET")
        return {
          endpoints: state.endpoints.filter((e) => e.project_id === project),
        };
      if (method === "POST") {
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
        );
      }
      const ep = state.endpoints.find(
        (e) => e.id === parts[4] && e.project_id === project,
      );
      if (!ep) throw Error("Endpoint not found.");
      ep.enabled = body.enabled;
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
