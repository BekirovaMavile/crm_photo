(function () {
  const form = document.getElementById("login-form");
  const username = document.getElementById("username");
  const password = document.getElementById("password");
  const btn = document.getElementById("login-btn");
  const errorEl = document.getElementById("login-error");

  function showError(msg) {
    errorEl.textContent = msg;
    errorEl.hidden = !msg;
  }

  form.addEventListener("submit", async (e) => {
    e.preventDefault();
    showError("");
    btn.disabled = true;

    try {
      const res = await fetch("/api/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        credentials: "include",
        body: JSON.stringify({
          username: username.value.trim(),
          password: password.value,
        }),
      });
      const data = await res.json().catch(() => ({}));
      if (!res.ok || !data.ok) {
        showError(data.error || "Неверный логин или пароль");
        btn.disabled = false;
        return;
      }
      window.location.href = "/";
    } catch (err) {
      showError(err.message || "Сеть недоступна");
      btn.disabled = false;
    }
  });
})();
