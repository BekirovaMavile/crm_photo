(function () {
  const cred = { credentials: "include" };

  const form = document.getElementById("form");
  const logoutBtn = document.getElementById("logout");
  const dateFrom = document.getElementById("date_from");
  const dateTo = document.getElementById("date_to");
  const btn = document.getElementById("btn");
  const progressEl = document.getElementById("progress");
  const statusLabel = document.getElementById("status-label");
  const counts = document.getElementById("counts");
  const bar = document.getElementById("bar");
  const hint = document.getElementById("hint");
  const errorEl = document.getElementById("error");

  const today = new Date();
  const iso = (d) => d.toISOString().slice(0, 10);
  dateTo.value = iso(today);
  const monthAgo = new Date(today);
  monthAgo.setMonth(monthAgo.getMonth() - 1);
  dateFrom.value = iso(monthAgo);

  let pollTimer = null;

  function showError(msg) {
    errorEl.textContent = msg;
    errorEl.hidden = !msg;
  }

  function setBar(done, total) {
    let pct = 0;
    if (total > 0) {
      pct = Math.min(100, Math.round((done / total) * 100));
    } else if (done === 0) {
      pct = 0;
    }
    bar.style.width = pct + "%";
    const outer = bar.parentElement;
    outer.setAttribute("aria-valuenow", String(pct));
    outer.setAttribute("aria-valuemax", "100");
  }

  function renderJob(job) {
    if (!job) {
      progressEl.classList.add("hidden");
      btn.disabled = false;
      return;
    }

    progressEl.classList.remove("hidden");

    const { status, done, total, message, error } = job;
    counts.textContent =
      total > 0
        ? `${done} / ${total} файлов`
        : done > 0
          ? `${done} файлов`
          : "0 файлов";

    if (status === "queued") {
      statusLabel.textContent = "В очереди…";
      hint.textContent = message || "Ожидание запуска";
      setBar(0, 0);
      btn.disabled = true;
    } else if (status === "running") {
      statusLabel.textContent = "Синхронизация…";
      hint.textContent = message || "";
      setBar(done, total);
      btn.disabled = true;
    } else if (status === "done") {
      statusLabel.textContent = "Готово";
      if (total === 0 && done === 0) {
        counts.textContent = "0 файлов";
        hint.textContent = "В выбранном периоде не найдено фото для загрузки.";
        setBar(0, 0);
      } else {
        hint.textContent = message || "Все файлы обработаны.";
        setBar(done, total);
      }
      stopPoll();
      btn.disabled = false;
    } else if (status === "error") {
      statusLabel.textContent = "Ошибка";
      showError(error || "Неизвестная ошибка");
      hint.textContent = message || "";
      setBar(done, total || 1);
      stopPoll();
      btn.disabled = false;
    }
  }

  function stopPoll() {
    if (pollTimer) {
      clearInterval(pollTimer);
      pollTimer = null;
    }
  }

  async function fetchJob(jobId) {
    const r = await fetch("/api/jobs/" + encodeURIComponent(jobId), cred);
    if (r.status === 401) {
      window.location.href = "/login";
      return;
    }
    const data = await r.json();
    if (!data.ok || !data.job) return;
    renderJob(data.job);
  }

  function poll(jobId) {
    stopPoll();
    fetchJob(jobId).catch(() => {});
    pollTimer = setInterval(() => {
      fetchJob(jobId).catch(() => {});
    }, 1000);
  }

  async function restoreActiveJob() {
    try {
      const r = await fetch("/api/active-job", cred);
      if (r.status === 401) {
        window.location.href = "/login";
        return;
      }
      const data = await r.json();
      if (data.ok && data.job && data.job.job_id) {
        renderJob(data.job);
        poll(data.job.job_id);
      }
    } catch (_) {
      /* ignore */
    }
  }

  logoutBtn.addEventListener("click", async () => {
    try {
      await fetch("/api/logout", { method: "POST", ...cred });
    } catch (_) {
      /* ignore */
    }
    window.location.href = "/login";
  });

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    showError("");
    progressEl.classList.remove("hidden");
    btn.disabled = true;
    statusLabel.textContent = "Запуск…";
    counts.textContent = "";
    hint.textContent = "";
    setBar(0, 0);

    try {
      const res = await fetch("/api/export", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        ...cred,
        body: JSON.stringify({
          date_from: dateFrom.value,
          date_to: dateTo.value,
        }),
      });

      const data = await res.json().catch(() => ({}));
      if (res.status === 401) {
        window.location.href = "/login";
        return;
      }
      if (res.status === 409 && data.active_job_id) {
        showError(data.error || "Экспорт уже запущен");
        poll(data.active_job_id);
        return;
      }
      if (!res.ok || !data.ok) {
        showError(data.error || "Не удалось запустить");
        btn.disabled = false;
        return;
      }
      poll(data.job_id);
    } catch (err) {
      showError(err.message || "Сеть недоступна");
      btn.disabled = false;
    }
  });

  restoreActiveJob();
})();