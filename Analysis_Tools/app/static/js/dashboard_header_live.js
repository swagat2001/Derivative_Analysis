// Use Server-Sent Events (SSE) for true real-time updates
const evtSource = new EventSource("/stream/live_indices");

let lastData = {};

evtSource.onmessage = function (event) {
  try {
    const data = JSON.parse(event.data);

    // update NIFTY
    updateValue("nifty-value", data.NIFTY, lastData.NIFTY);
    updateValue("banknifty-value", data.BANKNIFTY, lastData.BANKNIFTY);
    updateValue("gold-value", data.GOLD, lastData.GOLD);
    updateValue("silver-value", data.SILVER, lastData.SILVER);

    lastData = data;
  } catch (err) {
    console.error("Error parsing live stream data:", err, event.data);
  }
};

// Handle connection errors gracefully
evtSource.onerror = (err) => {
  console.warn("SSE connection lost, attempting to reconnect...", err);
};

// Helper: animate value change (optional, looks pro)
function updateValue(id, newVal, oldVal) {
  const el = document.getElementById(id);
  if (!el) return;

  el.innerText = newVal.toFixed(2);

  if (oldVal === undefined) return;

  const color = newVal > oldVal ? "limegreen" : newVal < oldVal ? "red" : null;
  if (color) {
    el.style.color = color;
    setTimeout(() => (el.style.color = ""), 400);
  }
}
