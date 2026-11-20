(function () {
  "use strict";

  var panel = document.getElementById("order-panel");
  var STATE_LABELS = { none: "未生效", prepared: "已预备", committed: "已提交" };

  function setText(id, text) {
    var el = document.getElementById(id);
    if (el) el.textContent = text;
  }

  function setTag(id, state) {
    var el = document.getElementById(id);
    if (!el) return;
    el.className = "state-tag state-" + state;
    el.textContent = STATE_LABELS[state] || state;
  }

  function overall(order) {
    if (order.registry_state === "committed" && order.quota_state === "committed") {
      return { text: "发布成功", cls: "status status-recovered" };
    }
    if (
      order.registry_state === "none" &&
      order.quota_state === "none" &&
      order.decision !== null
    ) {
      return { text: "已撤销", cls: "status status-revoked" };
    }
    if (order.registry_state === "none" && order.quota_state === "none") {
      return { text: "未生效", cls: "status status-none" };
    }
    return { text: "两阶段提交进行中", cls: "status status-releasing" };
  }

  function apply(order) {
    setTag("registry-state", order.registry_state);
    setTag("quota-state", order.quota_state);
    setText("remaining-quota", String(order.remaining_quota));
    var badge = document.getElementById("overall-badge");
    if (badge) {
      var verdict = overall(order);
      badge.className = verdict.cls;
      badge.textContent = verdict.text;
    }
    var listingRemaining = document.getElementById("listing-remaining");
    if (listingRemaining) listingRemaining.textContent = String(order.remaining_quota);
  }

  function poll() {
    if (!panel) return;
    var orderNo = encodeURIComponent(panel.dataset.orderNo);
    fetch("/release/api/orders/" + orderNo, {
      headers: { Accept: "application/json" },
      cache: "no-store",
    })
      .then(function (res) {
        if (!res.ok) throw new Error("bad status");
        return res.json();
      })
      .then(function (data) {
        if (data.ok) apply(data.order);
      })
      .catch(function () {
        /* transient error: keep polling */
      });
    setTimeout(poll, 2000);
  }

  if (panel) setTimeout(poll, 1500);
})();
