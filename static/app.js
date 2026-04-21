(function () {
  const cred = { credentials: "include" };

  const form = document.getElementById("form");
  const logoutBtn = document.getElementById("logout");
  const dateFrom = document.getElementById("date_from");
  const dateTo = document.getElementById("date_to");
  const btn = document.getElementById("btn");
  const pauseBtn = document.getElementById("btn-pause");
  const stopBtn = document.getElementById("btn-stop");
  const historyBtn = document.getElementById("btn-history");
  const historyClearBtn = document.getElementById("btn-history-clear");
  const deleteUploadedBtn = document.getElementById("btn-delete-uploaded");
  const progressEl = document.getElementById("progress");
  const statusLabel = document.getElementById("status-label");
  const counts = document.getElementById("counts");
  const bar = document.getElementById("bar");
  const hint = document.getElementById("hint");
  const errorEl = document.getElementById("error");
  const historyEl = document.getElementById("history");
  const historyListEl = document.getElementById("history-list");

  const today = new Date();
  const iso = (d) => d.toISOString().slice(0, 10);
  dateTo.value = iso(today);
  const monthAgo = new Date(today);
  monthAgo.setMonth(monthAgo.getMonth() - 1);
  dateFrom.value = iso(monthAgo);

  let pollTimer = null;
  let activeJobId = null;
  pauseBtn.disabled = true;
  stopBtn.disabled = true;
  deleteUploadedBtn.classList.add("hidden");

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
      pauseBtn.disabled = true;
      stopBtn.disabled = true;
      activeJobId = null;
      return;
    }

    progressEl.classList.remove("hidden");
    activeJobId = job.job_id || null;
    if (job.date_from) dateFrom.value = job.date_from;
    if (job.date_to) dateTo.value = job.date_to;

    const { status, done, total, message, error, control_status, cleanup_status, cleanup_done, cleanup_total } = job;
    if (cleanup_status === "running") {
      deleteUploadedBtn.classList.add("hidden");
      statusLabel.textContent = "Удаление фото из CRM…";
      const cDone = Number(cleanup_done || 0);
      const cTotal = Number(cleanup_total || 0);
      counts.textContent = cTotal > 0 ? `${cDone} / ${cTotal} полей` : `${cDone} полей`;
      hint.textContent = message || "Удаляю выгруженные фото из CRM";
      setBar(cDone, cTotal || 1);
      btn.disabled = true;
      pauseBtn.disabled = true;
      stopBtn.disabled = true;
      return;
    }
    counts.textContent =
      total > 0
        ? `${done} / ${total} файлов`
        : done > 0
          ? `${done} файлов`
          : "0 файлов";

    if (status === "queued" || (status === "running" && control_status === "paused")) {
      deleteUploadedBtn.classList.add("hidden");
      pauseBtn.disabled = false;
      stopBtn.disabled = false;
      pauseBtn.textContent = control_status === "paused" ? "Продолжить" : "Поставить на паузу";
      if (control_status === "paused") {
        statusLabel.textContent = "Пауза";
        hint.textContent = message || "Синхронизация приостановлена";
        btn.disabled = true;
        return;
      }
      statusLabel.textContent = "В очереди…";
      hint.textContent = message || "Ожидание запуска";
      setBar(0, 0);
      btn.disabled = true;
    } else if (status === "running") {
      deleteUploadedBtn.classList.add("hidden");
      pauseBtn.disabled = false;
      stopBtn.disabled = false;
      pauseBtn.textContent = "Поставить на паузу";
      statusLabel.textContent = "Синхронизация…";
      hint.textContent = message || "";
      setBar(done, total);
      btn.disabled = true;
    } else if (status === "done") {
      pauseBtn.disabled = true;
      stopBtn.disabled = true;
      pauseBtn.textContent = "Поставить на паузу";
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
      if (cleanup_status === "done") {
        hint.textContent = "Выгрузка завершена. Фото из CRM удалены.";
        deleteUploadedBtn.classList.add("hidden");
      } else {
        deleteUploadedBtn.classList.remove("hidden");
      }
    } else if (status === "error") {
      deleteUploadedBtn.classList.add("hidden");
      pauseBtn.disabled = true;
      stopBtn.disabled = true;
      pauseBtn.textContent = "Поставить на паузу";
      statusLabel.textContent = "Ошибка";
      showError(error || "Неизвестная ошибка");
      hint.textContent = message || "";
      setBar(done, total || 1);
      stopPoll();
      btn.disabled = false;
    } else if (status === "cancelled") {
      pauseBtn.disabled = true;
      stopBtn.disabled = true;
      pauseBtn.textContent = "Поставить на паузу";
      statusLabel.textContent = "Остановлено";
      hint.textContent = message || "Остановлено пользователем";
      stopPoll();
      btn.disabled = false;
      if (cleanup_status === "done") {
        hint.textContent = "Выгрузка остановлена. Фото из CRM удалены.";
        deleteUploadedBtn.classList.add("hidden");
      } else {
        deleteUploadedBtn.classList.remove("hidden");
      }
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

  async function postJobControl(action) {
    if (!activeJobId) return;
    const res = await fetch(`/api/jobs/${encodeURIComponent(activeJobId)}/${action}`, {
      method: "POST",
      ...cred,
    });
    if (res.status === 401) {
      window.location.href = "/login";
      return;
    }
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) {
      throw new Error(data.error || "Не удалось выполнить действие");
    }
    await fetchJob(activeJobId);
  }

  function fmtDate(value) {
    if (!value) return "—";
    return String(value).slice(0, 19).replace("T", " ");
  }

  function renderHistory(items) {
    if (!Array.isArray(items) || !items.length) {
      historyListEl.innerHTML = '<p class="hint">История пока пустая.</p>';
      return;
    }
    historyListEl.innerHTML = items.map((item) => {
      const recordsTotal = Number(item.records_streets || 0) + Number(item.records_spdp || 0);
      const attachmentsTotal = Number(item.attachments_streets || 0) + Number(item.attachments_spdp || 0);
      const wasStopped = item.status === "cancelled" || item.control_status === "cancelled";
      const stopText = wasStopped
        ? `Остановлена пользователем. Отправлено файлов: ${item.done || 0}.`
        : "Остановки не было.";
      const cleanupText = item.cleanup_status === "done"
        ? `Фото удалены из CRM. Удалено файлов: ${item.cleanup_deleted || 0}.`
        : "Фото из CRM не удалялись.";
      return `
        <div class="history-item">
          <div class="history-title">Период: ${item.date_from} - ${item.date_to}</div>
          <div class="history-status">Статус: ${item.status}</div>
          <div class="history-meta">Создано: ${fmtDate(item.created_at)} | Обновлено: ${fmtDate(item.updated_at)}</div>
          <div class="history-meta">${stopText}</div>
          <div class="history-meta">${cleanupText}</div>
          <div class="history-meta">Фото из улиц: ${item.attachments_streets || 0}</div>
          <div class="history-meta">Фото из СП/ДП: ${item.attachments_spdp || 0}</div>
          <div class="history-meta">Всего фото: ${attachmentsTotal}</div>
          <div class="history-meta">Записей в улицах: ${item.records_streets || 0}</div>
          <div class="history-meta">Записей в СП/ДП: ${item.records_spdp || 0}</div>
          <div class="history-meta">Всего записей: ${recordsTotal}</div>
          <div class="history-meta">Прогресс файлов: ${item.done || 0} / ${item.total || 0}</div>
          <button type="button" class="btn-text history-delete history-delete-danger" data-id="${item.job_id}">Удалить</button>
        </div>
      `;
    }).join("");
  }

  async function loadHistory() {
    const res = await fetch("/api/history", cred);
    if (res.status === 401) {
      window.location.href = "/login";
      return;
    }
    const data = await res.json().catch(() => ({}));
    if (!res.ok || !data.ok) {
      throw new Error(data.error || "Не удалось загрузить историю");
    }
    renderHistory(data.items || []);
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

  pauseBtn.addEventListener("click", async () => {
    showError("");
    try {
      const action = pauseBtn.textContent.includes("Продолжить") ? "resume" : "pause";
      await postJobControl(action);
    } catch (err) {
      showError(err.message || "Не удалось изменить состояние");
    }
  });

  stopBtn.addEventListener("click", async () => {
    showError("");
    try {
      await postJobControl("stop");
    } catch (err) {
      showError(err.message || "Не удалось остановить");
    }
  });

  deleteUploadedBtn.addEventListener("click", async () => {
    if (!activeJobId) return;
    if (!confirm("Удалить из CRM только те фото, которые были выгружены на диск?")) return;
    try {
      const res = await fetch(`/api/jobs/${encodeURIComponent(activeJobId)}/delete-uploaded`, {
        method: "POST",
        ...cred,
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data.ok) throw new Error(data.error || "Не удалось запустить удаление");
      await fetchJob(activeJobId);
      poll(activeJobId);
    } catch (err) {
      showError(err.message || "Не удалось запустить удаление");
    }
  });

  historyBtn.addEventListener("click", async () => {
    showError("");
    historyEl.classList.toggle("hidden");
    if (!historyEl.classList.contains("hidden")) {
      try {
        await loadHistory();
      } catch (err) {
        showError(err.message || "Не удалось загрузить историю");
      }
    }
  });

  historyClearBtn.addEventListener("click", async () => {
    if (!confirm("Удалить всю историю загрузок?")) return;
    try {
      const res = await fetch("/api/history", { method: "DELETE", ...cred });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data.ok) throw new Error(data.error || "Не удалось удалить историю");
      await loadHistory();
    } catch (err) {
      showError(err.message || "Не удалось удалить историю");
    }
  });

  historyListEl.addEventListener("click", async (e) => {
    const target = e.target;
    if (!(target instanceof HTMLElement)) return;
    if (!target.classList.contains("history-delete")) return;
    const jobId = target.getAttribute("data-id");
    if (!jobId) return;
    if (!confirm("Удалить эту запись из истории?")) return;
    try {
      const res = await fetch(`/api/history/${encodeURIComponent(jobId)}`, { method: "DELETE", ...cred });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data.ok) throw new Error(data.error || "Не удалось удалить запись");
      await loadHistory();
    } catch (err) {
      showError(err.message || "Не удалось удалить запись");
    }
  });

  restoreActiveJob();
})();