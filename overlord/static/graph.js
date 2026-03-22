(function () {
  const stage = document.getElementById("graph-stage");
  const detail = document.getElementById("node-detail");
  if (!stage || !detail) {
    return;
  }

  const edgeLayer = stage.querySelector(".graph-edges");
  const nodeLayer = stage.querySelector(".graph-nodes");
  const graphUrl = stage.dataset.graphUrl;
  const commandUrl = stage.dataset.commandUrl;
  const defaultRepoRoot = stage.dataset.defaultRepoRoot;
  const defaultGeneralPrefix = stage.dataset.defaultGeneralPrefix || "general-overlord";
  const defaultCaptainPrefix = stage.dataset.defaultCaptainPrefix || "captain-overlord";
  let graph = JSON.parse(stage.dataset.graph || "{}");
  let selectedNodeId = graph.selectedNodeId || null;
  const nodeEls = new Map();
  let plannerDraft = "";
  let plannerStatusText = "";
  let plannerRole = "general";
  let plannerFocused = false;
  let extendedDetailsOpen = false;

  const roleSizes = {
    overlord: 156,
    general: 96,
    captain: 82,
    worker: 66,
  };

  function render(nextGraph) {
    graph = nextGraph;
    if (!graph.nodes || !graph.edges) {
      return;
    }
    if (selectedNodeId && !graph.nodes.some((node) => node.id === selectedNodeId)) {
      selectedNodeId = graph.selectedNodeId || null;
    }
    const positions = computeLayout(graph);
    syncNodes(graph.nodes, positions);
    syncEdges(graph.edges, positions);
    if (!(plannerFocused && selectedNodeId === "overlord")) {
      renderDetail();
    }
  }

  function computeLayout(currentGraph) {
    const width = stage.clientWidth || 900;
    const height = stage.clientHeight || 760;
    const center = { x: width / 2, y: height / 2 };
    const positions = { overlord: center };
    const nodesById = new Map(currentGraph.nodes.map((node) => [node.id, node]));
    const childrenByParent = buildChildrenByParent(currentGraph.edges);
    const roots = (childrenByParent.get("overlord") || [])
      .map((id) => nodesById.get(id))
      .filter(Boolean)
      .sort((a, b) => a.id.localeCompare(b.id));

    const generalRadius = Math.min(width, height) * 0.2;
    const captainDistance = 88;
    const workerDistance = 76;

    roots.forEach((root, index) => {
      const baseAngle = angleForIndex(index, roots.length, -Math.PI / 2);
      positions[root.id] = project(center, baseAngle, generalRadius, width, height, 42);

      if (root.role === "captain") {
        layoutWorkers(root.id, baseAngle);
        return;
      }

      const captainIds = (childrenByParent.get(root.id) || [])
        .filter((id) => nodesById.get(id)?.role === "captain")
        .sort();
      captainIds.forEach((captainId, captainIndex) => {
        const captainAngle = spreadAngle(baseAngle, captainIndex, captainIds.length, 0.42);
        const captainOrigin = positions[root.id];
        positions[captainId] = project(captainOrigin, captainAngle, captainDistance, width, height, 42);
        layoutWorkers(captainId, captainAngle);
      });
    });

    function layoutWorkers(parentId, parentAngle) {
      const workerIds = (childrenByParent.get(parentId) || [])
        .filter((id) => nodesById.get(id)?.role === "worker")
        .sort();
      workerIds.forEach((workerId, workerIndex) => {
        const workerAngle = spreadAngle(parentAngle, workerIndex, workerIds.length, 0.52);
        const workerOrigin = positions[parentId];
        positions[workerId] = project(
          workerOrigin,
          workerAngle,
          workerDistance,
          width,
          height,
          42
        );
      });
    }

    currentGraph.nodes.forEach((node) => {
      if (!positions[node.id]) {
        positions[node.id] = center;
      }
      positions[node.id].size = roleSizes[node.role] || 82;
    });
    return positions;
  }

  function buildChildrenByParent(edges) {
    const childrenByParent = new Map();
    edges.forEach((edge) => {
      if (!childrenByParent.has(edge.source)) {
        childrenByParent.set(edge.source, []);
      }
      childrenByParent.get(edge.source).push(edge.target);
    });
    return childrenByParent;
  }

  function angleForIndex(index, count, phase) {
    return phase + (Math.PI * 2 * index) / Math.max(1, count);
  }

  function spreadAngle(baseAngle, index, count, spread) {
    if (count <= 1) {
      return baseAngle;
    }
    return baseAngle - spread / 2 + (spread * index) / (count - 1);
  }

  function project(origin, angle, distance, width, height, margin) {
    return {
      x: clamp(origin.x + Math.cos(angle) * distance, margin, width - margin),
      y: clamp(origin.y + Math.sin(angle) * distance, margin, height - margin),
    };
  }

  function syncNodes(nodes, positions) {
    const activeIds = new Set();
    nodes.forEach((node) => {
      activeIds.add(node.id);
      let el = nodeEls.get(node.id);
      if (!el) {
        el = document.createElement("button");
        el.type = "button";
        el.className = "graph-node";
        el.dataset.nodeId = node.id;
        el.innerHTML = `
          <div class="graph-node-inner">
            <div class="graph-node-title"></div>
            <div class="graph-node-meta graph-node-status"><i class="status-dot"></i><span></span></div>
            <div class="graph-node-meta graph-node-age"></div>
          </div>
        `;
        el.addEventListener("click", (event) => {
          event.stopPropagation();
          selectedNodeId = node.id;
          render(graph);
        });
        nodeEls.set(node.id, el);
        nodeLayer.appendChild(el);
      }
      const pos = positions[node.id];
      el.className = `graph-node ${node.role} state-${node.state}${node.id === selectedNodeId ? " graph-node-selected" : ""}`;
      el.style.width = `${pos.size}px`;
      el.style.height = `${pos.size}px`;
      el.style.left = `${pos.x}px`;
      el.style.top = `${pos.y}px`;
      el.querySelector(".graph-node-title").textContent = node.display_type || node.role;
      el.querySelector(".graph-node-status span").textContent = node.status_label || node.state;
      el.querySelector(".graph-node-age").textContent = node.age_label || "";
    });

    Array.from(nodeEls.keys()).forEach((id) => {
      if (!activeIds.has(id)) {
        nodeEls.get(id).remove();
        nodeEls.delete(id);
      }
    });
  }

  function syncEdges(edges, positions) {
    edgeLayer.innerHTML = "";
    edges.forEach((edge) => {
      const source = positions[edge.source];
      const target = positions[edge.target];
      if (!source || !target) {
        return;
      }
      const line = document.createElementNS("http://www.w3.org/2000/svg", "line");
      line.setAttribute("class", "graph-line");
      line.setAttribute("x1", source.x);
      line.setAttribute("y1", source.y);
      line.setAttribute("x2", target.x);
      line.setAttribute("y2", target.y);
      edgeLayer.appendChild(line);
    });
  }

  function renderDetail() {
    if (!selectedNodeId) {
      detail.classList.add("detail-card-hidden");
      detail.innerHTML = "";
      return;
    }
    preserveDetailState();
    const node = (graph.nodes || []).find((entry) => entry.id === selectedNodeId);
    if (!node) {
      detail.classList.add("detail-card-hidden");
      detail.innerHTML = "";
      return;
    }
    const messages = (node.detail.messages || [])
      .map((item) => `
        <article class="message-item">
          <div class="message-head">
            <span class="message-sender">${escapeHtml(item.sender || "")}</span>
            <span class="message-type">${escapeHtml(item.type || "")}</span>
            <span class="message-age">${escapeHtml(item.age || "")}</span>
          </div>
          <p class="message-body">${escapeHtml(item.body || "")}</p>
        </article>
      `)
      .join("");
    const extended = (node.detail.extended || [])
      .map((item) => `<div class="meta-item"><span class="label">${escapeHtml(item.label)}</span><p>${escapeHtml(item.value || "")}</p></div>`)
      .join("");
    const actions = (node.detail.actions || [])
      .map((item) => `<a class="action-link" href="${escapeAttr(item.href)}">${escapeHtml(item.label)}</a>`)
      .join("");
    const missionPlanner = node.id === "overlord" ? `
      <div class="mission-planner">
        <h3>Mission Planner</h3>
        <label class="planner-label" for="mission-role">Dispatch role</label>
        <select id="mission-role" class="planner-select">
          <option value="general">General</option>
          <option value="captain">Captain</option>
        </select>
        <textarea id="mission-input" placeholder="Describe the mission to dispatch."></textarea>
        <div class="planner-actions">
          <button type="button" class="planner-button planner-button-primary" id="mission-submit">Submit</button>
          <button type="button" class="planner-button" id="mission-cancel">Cancel</button>
        </div>
        <p class="planner-status" id="mission-status"></p>
      </div>
    ` : "";
    detail.innerHTML = `
      <p class="eyebrow">${escapeHtml(node.detail.subtitle || node.role)}</p>
      <h3>${escapeHtml(node.detail.title || node.label)}</h3>
      <div class="detail-primary">
        <div class="primary-item"><span class="label">State</span><p>${escapeHtml(node.detail.state || node.status_label || "")}</p></div>
        <div class="primary-item"><span class="label">Mission</span><p>${escapeHtml(node.detail.mission || "")}</p></div>
      </div>
      <section class="message-feed">
        <div class="message-feed-head">
          <span class="label">Messages</span>
        </div>
        <div class="message-list">${messages || '<p class="message-empty">No messages yet.</p>'}</div>
      </section>
      ${actions ? `<div class="action-row">${actions}</div>` : ""}
      <section class="extended-details">
        <button type="button" class="details-toggle" id="details-toggle" aria-expanded="${extendedDetailsOpen ? "true" : "false"}">Extended Details</button>
        <div class="meta-list${extendedDetailsOpen ? "" : " meta-list-hidden"}" id="details-body">${extended}</div>
      </section>
      ${missionPlanner}
    `;
    wireExtendedDetails();
    if (node.id === "overlord") {
      wireMissionPlanner();
    }
    detail.classList.remove("detail-card-hidden");
  }

  function preserveDetailState() {
    const input = detail.querySelector("#mission-input");
    const roleSelect = detail.querySelector("#mission-role");
    const status = detail.querySelector("#mission-status");
    if (input) {
      plannerDraft = input.value;
    }
    if (roleSelect) {
      plannerRole = roleSelect.value;
    }
    if (status) {
      plannerStatusText = status.textContent || "";
    }
  }

  function wireMissionPlanner() {
    const submit = detail.querySelector("#mission-submit");
    const cancel = detail.querySelector("#mission-cancel");
    const input = detail.querySelector("#mission-input");
    const roleSelect = detail.querySelector("#mission-role");
    const status = detail.querySelector("#mission-status");
    if (!submit || !cancel || !input || !roleSelect || !status) {
      return;
    }
    input.value = plannerDraft;
    roleSelect.value = plannerRole;
    status.textContent = plannerStatusText;
    input.addEventListener("input", () => {
      plannerDraft = input.value;
    });
    input.addEventListener("focus", () => {
      plannerFocused = true;
    });
    input.addEventListener("blur", () => {
      plannerFocused = false;
    });
    roleSelect.addEventListener("change", () => {
      plannerRole = roleSelect.value;
    });
    submit.addEventListener("click", async (event) => {
      event.stopPropagation();
      const instruction = input.value.trim();
      if (!instruction) {
        status.textContent = "Mission text is required.";
        plannerStatusText = status.textContent;
        return;
      }
      const dispatchRole = roleSelect.value === "captain" ? "captain" : "general";
      const workerPrefix = dispatchRole === "captain" ? defaultCaptainPrefix : defaultGeneralPrefix;
      const generalWorkerId = `${workerPrefix}-${Date.now().toString(36)}`;
      status.textContent = `Launching ${dispatchRole}...`;
      plannerStatusText = status.textContent;
      submit.disabled = true;
      cancel.disabled = true;
      try {
        const response = await fetch(commandUrl, {
          method: "POST",
          headers: {
            "Content-Type": "application/json",
            Accept: "application/json",
          },
          body: JSON.stringify({
            general_worker_id: generalWorkerId,
            dispatch_role: dispatchRole,
            repo_path: defaultRepoRoot,
            branch_hint: null,
            operator_instruction: instruction,
          }),
        });
        if (!response.ok) {
          const payload = await response.json().catch(() => ({}));
          status.textContent = payload.detail || "Launch failed.";
          plannerStatusText = status.textContent;
          return;
        }
        const payload = await response.json();
        status.textContent = `Launched ${payload.command.dispatch_role} ${payload.command.general_worker_id} (pid ${payload.command.pid}).`;
        plannerStatusText = status.textContent;
        input.value = "";
        plannerDraft = "";
        plannerRole = "general";
        plannerFocused = false;
        refreshGraph();
      } catch (error) {
        status.textContent = "Launch failed.";
        plannerStatusText = status.textContent;
      } finally {
        submit.disabled = false;
        cancel.disabled = false;
      }
    });
    cancel.addEventListener("click", (event) => {
      event.stopPropagation();
      plannerDraft = "";
      plannerStatusText = "";
      plannerRole = "general";
      plannerFocused = false;
      selectedNodeId = null;
      render(graph);
    });
  }

  function wireExtendedDetails() {
    const toggle = detail.querySelector("#details-toggle");
    const body = detail.querySelector("#details-body");
    if (!toggle || !body) {
      return;
    }
    toggle.addEventListener("click", (event) => {
      event.stopPropagation();
      extendedDetailsOpen = !extendedDetailsOpen;
      toggle.setAttribute("aria-expanded", extendedDetailsOpen ? "true" : "false");
      body.classList.toggle("meta-list-hidden", !extendedDetailsOpen);
    });
  }

  async function refreshGraph() {
    try {
      const response = await fetch(graphUrl, { headers: { Accept: "application/json" } });
      if (!response.ok) {
        return;
      }
      const payload = await response.json();
      render(payload.graph);
    } catch (error) {
      // Keep the last rendered graph if polling fails.
    }
  }

  function escapeHtml(value) {
    return String(value)
      .replaceAll("&", "&amp;")
      .replaceAll("<", "&lt;")
      .replaceAll(">", "&gt;")
      .replaceAll('"', "&quot;");
  }

  function escapeAttr(value) {
    return escapeHtml(value).replaceAll("'", "&#39;");
  }

  function clamp(value, min, max) {
    return Math.max(min, Math.min(max, value));
  }

  render(graph);
  detail.addEventListener("click", (event) => {
    event.stopPropagation();
  });
  stage.addEventListener("click", () => {
    if (!selectedNodeId) {
      return;
    }
    selectedNodeId = null;
    render(graph);
  });
  window.addEventListener("resize", () => render(graph));
  window.setInterval(refreshGraph, 5000);
})();
