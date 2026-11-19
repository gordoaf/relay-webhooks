/* The console uses the same interface for the server and the standalone demonstration. */
(() => {
  "use strict";
  const $ = (s) => document.querySelector(s);
  const $$ = (s) => [...document.querySelectorAll(s)];
  const escape = (value) =>
    String(value ?? "").replace(
      /[&<>"']/g,
      (c) =>
        ({
          "&": "&amp;",
          "<": "&lt;",
          ">": "&gt;",
          '"': "&quot;",
          "'": "&#39;",
        })[c],
    );
  const state = {
    project: "",
    projects: [],
    view: "deliveries",
    status: "",
    search: "",
    page: 0,
    lab: false,
    online: true,
    detail: null,
    busy: false,
    authenticated: false,
  };
  const pageSize = 12;
  let submitAction, toastTimer, searchTimer;
  function notify(message, error = false) {
    const box = $("#toast");
    box.textContent = message;
    box.classList.toggle("error", error);
    box.hidden = false;
    clearTimeout(toastTimer);
    toastTimer = setTimeout(() => {
      box.hidden = true;
    }, 4500);
  }
  async function request(path, method = "GET", body) {
    if (window.RELAY_DEMO) return window.relayDemo.request(path, method, body);
    const options = {
      method,
      credentials: "same-origin",
      headers: { "X-Relay-Request": "1" },
    };
    if (body !== undefined) {
      options.body = JSON.stringify(body);
      options.headers["Content-Type"] = "application/json";
    }
    const response = await fetch(path, options);
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      if (response.status === 401 && path !== "/api/session") showLogin();
      const detail =
        typeof data.detail === "string"
          ? data.detail
          : "Check the values and try again.";
      throw new Error(detail);
    }
    return data;
  }
  const base = () => "/api/projects/" + encodeURIComponent(state.project);
  const when = (stamp) => {
    const seconds = Math.max(0, Date.now() / 1000 - stamp);
    if (seconds < 10) return "Just now";
    if (seconds < 60) return Math.floor(seconds) + "s ago";
    if (seconds < 3600) return Math.floor(seconds / 60) + "m ago";
    if (seconds < 86400) return Math.floor(seconds / 3600) + "h ago";
    return new Date(stamp * 1000).toLocaleDateString();
  };
  const badge = (status) =>
    `<span class="badge ${escape(status)}">${escape(status[0].toUpperCase() + status.slice(1))}</span>`;
  function showLogin() {
    state.authenticated = false;
    if (!$("#login-dialog").open) $("#login-dialog").showModal();
  }
  async function loadProjects(selectId) {
    const data = await request("/api/projects");
    state.projects = data.projects;
    state.project =
      selectId ||
      (data.projects.some((p) => p.id === state.project)
        ? state.project
        : data.projects[0]?.id || "");
    const select = $("#project-select");
    select.replaceChildren();
    if (!data.projects.length) select.add(new Option("No projects yet", ""));
    for (const p of data.projects) select.add(new Option(p.name, p.id));
    select.value = state.project;
    state.page = 0;
  }
  function zeroStats() {
    for (const id of ["total", "delivered", "pending", "failed"])
      $("#stat-" + id).textContent = "0";
    $("#stat-success").textContent = "0% delivery rate";
  }
  async function refresh() {
    if (!state.authenticated || state.busy) return;
    state.busy = true;
    try {
      if (state.lab) {
        const receiver = await request("/api/demo/receiver");
        state.online = receiver.online;
        $("#toggle-receiver").classList.toggle("offline", !state.online);
        $("#toggle-receiver span:last-child").textContent =
          "Receiver " + (state.online ? "online" : "offline");
        $("#toggle-receiver").setAttribute(
          "aria-pressed",
          String(!state.online),
        );
      }
      if (!state.project) {
        zeroStats();
        $("#delivery-rows").replaceChildren();
        $("#empty-deliveries").hidden = false;
        $("#endpoint-list").innerHTML =
          '<div class="panel empty-state"><h3>Create a project to get started</h3><p>Projects keep endpoint configuration and event keys separate.</p><button class="button" id="create-first-project">Create project</button></div>';
        $("#create-first-project").onclick = createProject;
        $("#results-count").textContent = "0 deliveries";
        return;
      }
      const query = new URLSearchParams({
        limit: pageSize,
        offset: state.page * pageSize,
      });
      if (state.status) query.set("status", state.status);
      if (state.search) query.set("search", state.search);
      const projectId = state.project;
      const [stats, deliveries, endpoints] = await Promise.all([
        request(base() + "/stats"),
        request(base() + "/deliveries?" + query),
        request(base() + "/endpoints"),
      ]);
      if (state.project !== projectId) return;
      $("#stat-total").textContent = stats.total.toLocaleString();
      $("#stat-delivered").textContent = stats.delivered.toLocaleString();
      $("#stat-pending").textContent = (
        stats.pending +
        stats.processing +
        stats.retrying
      ).toLocaleString();
      $("#stat-failed").textContent = stats.failed.toLocaleString();
      $("#stat-success").textContent =
        stats.success_rate.toFixed(1) + "% delivery rate";
      $("#nav-count").textContent = stats.total;
      $("#delivery-rows").innerHTML = deliveries.items
        .map(
          (d) =>
            `<tr><td><div class="event-type">${escape(d.event_type)}</div><div class="event-id">${escape(d.event_id.slice(0, 19))}</div></td><td><div class="destination" title="${escape(d.endpoint_url)}">${escape(d.endpoint_url.replace(/^https?:\/\//, ""))}</div></td><td>${badge(d.status)}</td><td class="attempt-count">${d.attempt_count}<span class="time"> attempt${d.attempt_count === 1 ? "" : "s"}</span></td><td class="time" title="${escape(new Date(d.created_at * 1000).toLocaleString())}">${when(d.created_at)}</td><td><button class="detail-button" data-delivery="${escape(d.id)}" aria-label="Inspect ${escape(d.event_type)} delivery">⋯</button></td></tr>`,
        )
        .join("");
      $("#empty-deliveries").hidden = deliveries.total !== 0;
      $("#empty-deliveries h3").textContent =
        state.status || state.search
          ? "No matching deliveries"
          : "No deliveries here yet";
      $("#empty-deliveries p").textContent =
        state.status || state.search
          ? "Try another filter or search."
          : "Add an endpoint, then send your first event.";
      $("#empty-add-endpoint").hidden = !!(state.status || state.search);
      $("#results-count").textContent = deliveries.total
        ? `${state.page * pageSize + 1}–${state.page * pageSize + deliveries.items.length} of ${deliveries.total} deliveries`
        : "0 deliveries";
      $("#page-label").textContent = "Page " + (state.page + 1);
      $("#previous-page").disabled = state.page === 0;
      $("#next-page").disabled =
        (state.page + 1) * pageSize >= deliveries.total;
      $("#endpoint-list").innerHTML = endpoints.endpoints.length
        ? endpoints.endpoints
            .map(
              (e) =>
                `<article class="panel endpoint-card"><div><h2>${escape(e.url)}</h2><p>${escape(e.description || "Webhook destination")}</p><small>${e.rate_limit} requests / second · ${escape(e.id)}</small></div><div class="endpoint-actions">${badge(
                  e.enabled ? "delivered" : "pending",
                )
                  .replace("Delivered", "Enabled")
                  .replace(
                    "Pending",
                    "Paused",
                  )}<button class="button" data-endpoint="${escape(e.id)}" data-enabled="${e.enabled ? "1" : "0"}">${e.enabled ? "Pause" : "Enable"}</button></div></article>`,
            )
            .join("")
        : '<div class="panel empty-state"><div class="empty-icon">⌘</div><h3>Add your first endpoint</h3><p>Relay sends a copy of each new event to every enabled destination.</p><button id="first-endpoint" class="button">Add endpoint</button></div>';
      if ($("#first-endpoint")) $("#first-endpoint").onclick = addEndpoint;
      $("#last-updated").textContent =
        "Last updated " +
        new Date().toLocaleTimeString([], {
          hour: "2-digit",
          minute: "2-digit",
          second: "2-digit",
        });
      $("#connection-label").textContent = window.RELAY_DEMO
        ? "Browser demonstration"
        : "Console connected";
    } catch (error) {
      $("#connection-label").textContent = "Connection interrupted";
      notify(error.message, true);
    } finally {
      state.busy = false;
    }
  }
  function setView(view) {
    state.view = view;
    $$(".view").forEach((el) => (el.hidden = el.id !== "view-" + view));
    $$(".nav-item").forEach((el) =>
      el.classList.toggle("active", el.dataset.view === view),
    );
    $("#breadcrumb-current").textContent =
      view[0].toUpperCase() + view.slice(1);
    location.hash = view;
    refresh();
  }
  function openForm(title, fields, action, label = "Save") {
    $("#form-title").textContent = title;
    $("#form-fields").innerHTML = fields;
    $("#form-error").textContent = "";
    $("#form-submit").textContent = label;
    submitAction = action;
    $("#form-dialog").showModal();
  }
  function reveal(title, value, description) {
    $("#secret-title").textContent = title;
    $("#secret-value").textContent = value;
    $("#secret-description").textContent =
      description || "This secret is shown once. Save it somewhere secure.";
    $("#secret-dialog").showModal();
  }
  function createProject() {
    openForm(
      "Create a project",
      '<label for="project-name">Project name</label><input id="project-name" name="name" required maxlength="120" placeholder="Acme production">',
      async (form) => {
        const p = await request("/api/projects", "POST", {
          name: form.get("name").trim(),
        });
        await loadProjects(p.id);
        reveal("Save your project API key", p.api_key);
        await refresh();
      },
      "Create project",
    );
  }
  function addEndpoint() {
    if (!state.project) return createProject();
    openForm(
      "Add an endpoint",
      '<label for="endpoint-url">Destination URL</label><input id="endpoint-url" name="url" type="url" required placeholder="https://api.example.com/webhooks"><label for="endpoint-description">Description</label><input id="endpoint-description" name="description" maxlength="500" placeholder="Order fulfillment"><label for="endpoint-rate">Maximum requests per second</label><input id="endpoint-rate" name="rate_limit" type="number" min="1" max="1000" value="10" required>',
      async (form) => {
        const e = await request(base() + "/endpoints", "POST", {
          url: form.get("url"),
          description: form.get("description"),
          rate_limit: Number(form.get("rate_limit")),
        });
        reveal(
          "Save the signing secret",
          e.signing_secret,
          "Use this secret in your receiver to verify webhook signatures. It is shown once.",
        );
        await refresh();
      },
      "Add endpoint",
    );
  }
  function sendEvent() {
    if (!state.project) return createProject();
    openForm(
      "Send an event",
      '<label for="event-type">Event type</label><input id="event-type" name="type" required value="order.created" pattern="[A-Za-z][A-Za-z0-9_.:-]*"><label for="event-payload">Payload · JSON object</label><textarea id="event-payload" name="payload" spellcheck="false">{\n  "order_id": "order_1042",\n  "amount": 2400,\n  "currency": "USD"\n}</textarea>',
      async (form) => {
        let payload;
        try {
          payload = JSON.parse(form.get("payload"));
        } catch {
          throw new Error("Payload must be valid JSON.");
        }
        if (!payload || typeof payload !== "object" || Array.isArray(payload))
          throw new Error("Payload must be a JSON object.");
        const result = await request(base() + "/events", "POST", {
          type: form.get("type"),
          payload,
        });
        notify(
          result.event.delivery_count
            ? `Event accepted · ${result.event.delivery_count} delivery queued`
            : "Event saved. Add an enabled endpoint before sending another event.",
        );
        state.page = 0;
        await refresh();
      },
      "Send event",
    );
  }
  async function showDetail(id) {
    try {
      const d = await request(base() + "/deliveries/" + id);
      state.detail = id;
      $("#detail-title").textContent = d.event_type;
      $("#detail-content").innerHTML =
        `${badge(d.status)}<div class="detail-grid"><div><span>Event ID</span><strong>${escape(d.event_id)}</strong></div><div><span>Destination</span><strong>${escape(d.endpoint_url)}</strong></div><div><span>Created</span><strong>${escape(new Date(d.created_at * 1000).toLocaleString())}</strong></div><div><span>Next attempt</span><strong>${d.next_attempt_at ? escape(new Date(d.next_attempt_at * 1000).toLocaleTimeString()) : "—"}</strong></div></div><h2>Event data</h2><pre>${escape(JSON.stringify(d.payload, null, 2))}</pre><h2>Attempt history</h2>${d.attempts.length ? d.attempts.map((a) => `<article class="attempt"><div class="attempt-top"><strong>Attempt ${a.attempt_number} · ${a.status_code ? "HTTP " + a.status_code : "Network error"}</strong><span>${Number(a.duration_ms).toFixed(0)} ms</span></div><p>${escape(new Date(a.created_at * 1000).toLocaleString())}${a.error ? " · " + escape(a.error) : ""}</p>${a.response_excerpt ? "<pre>" + escape(a.response_excerpt) + "</pre>" : ""}</article>`).join("") : '<p class="hint">Waiting for the first completed attempt.</p>'}`;
      $("#replay-delivery").disabled = d.status === "processing";
      if (!$("#detail-dialog").open) $("#detail-dialog").showModal();
    } catch (error) {
      notify(error.message, true);
    }
  }
  async function copy(text) {
    try {
      await navigator.clipboard.writeText(text);
      notify("Copied to clipboard");
    } catch {
      notify("Clipboard unavailable. Select and copy the text manually.", true);
    }
  }
  async function initialize() {
    try {
      const session = await request("/api/session");
      state.authenticated = true;
      state.lab = session.demo_enabled;
      $("#lab").hidden = !state.lab;
      if (window.RELAY_DEMO) {
        $("#mode-badge").textContent = "INTERACTIVE DEMO";
        $("#lab-title").textContent = "A small failure. A complete recovery.";
        $("#lab-description").textContent =
          "Simulated deliveries in your browser. Take the receiver offline, send an event, then bring it back.";
        $("#setup-lab").hidden = true;
        $("#reset-demo").hidden = false;
        $("#logout").hidden = true;
      } else if (state.lab) {
        $("#mode-badge").textContent = "LOCAL LAB";
        $("#lab-description").textContent =
          "Real HTTP requests to a local receiver. Set up the lab, then take the receiver offline.";
      }
      await loadProjects();
      const origin = window.RELAY_DEMO
        ? "http://127.0.0.1:8787"
        : location.origin;
      $("#curl-example").textContent =
        `curl -X POST ${origin}/v1/events \\\n  -H 'Authorization: Bearer YOUR_PROJECT_KEY' \\\n  -H 'Content-Type: application/json' \\\n  -H 'Idempotency-Key: order-1042' \\\n  -d '{"type":"order.created","payload":{\n    "order_id":"order_1042",\n    "amount":2400,"currency":"USD"\n  }}'`;
      setView(
        ["deliveries", "endpoints", "quickstart"].includes(
          location.hash.slice(1),
        )
          ? location.hash.slice(1)
          : "deliveries",
      );
    } catch {
      showLogin();
    }
  }
  $("#login-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    $("#login-error").textContent = "";
    try {
      await request("/api/session", "POST", { token: $("#admin-token").value });
      $("#admin-token").value = "";
      $("#login-dialog").close();
      await initialize();
    } catch (error) {
      $("#login-error").textContent = error.message;
    }
  });
  $("#login-dialog").addEventListener("cancel", (e) => e.preventDefault());
  $("#action-form").addEventListener("submit", async (event) => {
    event.preventDefault();
    $("#form-submit").disabled = true;
    $("#form-error").textContent = "";
    try {
      await submitAction(new FormData(event.target));
      $("#form-dialog").close();
    } catch (error) {
      $("#form-error").textContent = error.message;
    } finally {
      $("#form-submit").disabled = false;
    }
  });
  $$(".close-dialog").forEach(
    (b) => (b.onclick = () => $("#form-dialog").close()),
  );
  $("#close-secret").onclick = () => {
    $("#secret-dialog").close();
  };
  $("#secret-dialog").addEventListener("close", () => {
    $("#secret-value").textContent = "";
  });
  $("#close-detail").onclick = () => $("#detail-dialog").close();
  $("#new-project").onclick = createProject;
  $("#add-endpoint").onclick = addEndpoint;
  $("#empty-add-endpoint").onclick = addEndpoint;
  $("#send-event").onclick = sendEvent;
  $("#refresh").onclick = refresh;
  $$("[data-view]").forEach((b) => (b.onclick = () => setView(b.dataset.view)));
  $$(".tab").forEach(
    (b) =>
      (b.onclick = () => {
        state.status = b.dataset.status;
        state.page = 0;
        $$(".tab").forEach((t) => t.classList.toggle("active", t === b));
        refresh();
      }),
  );
  $("#search").oninput = () => {
    clearTimeout(searchTimer);
    searchTimer = setTimeout(() => {
      state.search = $("#search").value;
      state.page = 0;
      refresh();
    }, 200);
  };
  $("#project-select").onchange = () => {
    state.project = $("#project-select").value;
    state.page = 0;
    refresh();
  };
  $("#previous-page").onclick = () => {
    state.page = Math.max(0, state.page - 1);
    refresh();
  };
  $("#next-page").onclick = () => {
    state.page++;
    refresh();
  };
  $("#delivery-rows").onclick = (e) => {
    const b = e.target.closest("[data-delivery]");
    if (b) showDetail(b.dataset.delivery);
  };
  $("#endpoint-list").onclick = async (e) => {
    const b = e.target.closest("[data-endpoint]");
    if (!b) return;
    try {
      await request(base() + "/endpoints/" + b.dataset.endpoint, "PATCH", {
        enabled: b.dataset.enabled !== "1",
      });
      await refresh();
    } catch (error) {
      notify(error.message, true);
    }
  };
  $("#replay-delivery").onclick = async () => {
    try {
      await request(base() + "/deliveries/" + state.detail + "/replay", "POST");
      notify("Delivery queued for replay");
      await refresh();
      await showDetail(state.detail);
    } catch (error) {
      notify(error.message, true);
    }
  };
  $("#setup-lab").onclick = async () => {
    try {
      const result = await request("/api/demo/setup", "POST");
      await loadProjects(result.project.id);
      reveal(
        "Local lab is ready",
        result.api_key,
        "This key belongs to the local demo shop. Use it to send events from your terminal.",
      );
      await refresh();
    } catch (error) {
      notify(error.message, true);
    }
  };
  $("#toggle-receiver").onclick = async () => {
    try {
      await request("/api/demo/receiver", "POST", { online: !state.online });
      await refresh();
      notify(
        state.online
          ? "Receiver is back online. Queued deliveries will retry."
          : "Receiver is offline. Send an event to observe retries.",
      );
    } catch (error) {
      notify(error.message, true);
    }
  };
  $("#lab-send").onclick = async () => {
    try {
      await request("/api/demo/send", "POST", {
        project_id: state.project,
        count: 1,
      });
      state.page = 0;
      state.status = "";
      $$(".tab").forEach((t) =>
        t.classList.toggle("active", t.dataset.status === ""),
      );
      setView("deliveries");
      notify("Test event queued");
      await refresh();
    } catch (error) {
      notify(error.message, true);
    }
  };
  $("#reset-demo").onclick = async () => {
    window.relayDemo.reset();
    state.project = "";
    await loadProjects();
    await refresh();
    notify("Demo reset");
  };
  $("#copy-curl").onclick = () => copy($("#curl-example").textContent);
  $("#copy-secret").onclick = () => copy($("#secret-value").textContent);
  $("#logout").onclick = async () => {
    try {
      await request("/api/session", "DELETE");
      showLogin();
    } catch (error) {
      notify(error.message, true);
    }
  };
  setInterval(() => {
    if (
      !document.hidden &&
      !$("#form-dialog").open &&
      !$("#login-dialog").open &&
      !document.activeElement?.closest("#delivery-rows, #endpoint-list")
    )
      refresh();
  }, 2000);
  initialize();
})();
