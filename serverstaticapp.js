const signalsDiv = document.getElementById("signals");
const refreshBtn = document.getElementById("refresh");

// 🚀 Подключение к Python-серверу (например FastAPI)
const API_URL = "https://yourserver.com/signals"; // здесь вставь API

async function loadSignals() {
  try {
    signalsDiv.innerHTML = "<p>⏳ Загружаем...</p>";

    // Если у тебя ещё нет сервера – можно пока тестово вставить mock-данные
    // const response = await fetch(API_URL);
    // const data = await response.json();

    const data = [
      { pair: "BTC/USDT", exchange: "Binance", price: 25000 },
      { pair: "ETH/USDT", exchange: "Bybit", price: 1600 }
    ];

    signalsDiv.innerHTML = "";
    data.forEach(sig => {
      const el = document.createElement("div");
      el.className = "signal";
      el.innerHTML = `🔹 <b>${sig.pair}</b><br>
                      Биржа: ${sig.exchange}<br>
                      Цена: ${sig.price} USDT`;
      signalsDiv.appendChild(el);
    });

  } catch (e) {
    signalsDiv.innerHTML = "<p>⚠️ Ошибка загрузки данных</p>";
  }
}

refreshBtn.addEventListener("click", loadSignals);

// Загружаем при старте
loadSignals();
