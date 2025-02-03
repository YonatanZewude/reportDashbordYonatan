const express = require("express");
const app = express();

app.get("/", (req, res) => {
  res.send("Reports Dashboard Microservice är igång! 🚀");
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => console.log(`✅ Server körs på port ${PORT}`));
