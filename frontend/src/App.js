import React, { useEffect, useState } from "react";
import "./App.css";

function App() {
  const [netSales, setNetSales] = useState(null);

  useEffect(() => {
    // If your Flask server is at :5000, adapt the IP or domain here
    fetch("http://<YOUR_EC2_PUBLIC_IP_OR_DOMAIN>:5000/net_sales")
      .then((res) => res.json())
      .then((data) => setNetSales(data.total_net_sales))
      .catch((err) => console.error(err));
  }, []);

  return (
    <div className="App">
      <h1>Net Sales Dashboard</h1>
      {netSales !== null ? (
        <p>Total Net Sales: ${netSales.toFixed(2)}</p>
      ) : (
        <p>Loading...</p>
      )}
    </div>
  );
}

export default App;

