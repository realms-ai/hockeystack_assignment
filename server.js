const bodyParser = require('body-parser');
const cookieParser = require('cookie-parser');
const express = require('express');
const http = require('http');
const moment = require('moment');
const axios = require('axios');
axios.defaults.baseURL = 'https://api.hubapi.com';
axios.defaults.headers.post['Content-Type'] = 'application/x-www-form-urlencoded';
const {
  PORT,
  NODE_ENV,
  HUBSPOT_CID,
  HUBSPOT_CS,
  HUBSPOT_REFRESH_TOKEN
} = process.env;
const REDIRECT_URI = `http://localhost:${PORT}/oauth-callback`;

// server setup
const app = express();
const server = http.Server(app);

app.locals.moment = moment;
app.locals.version = process.env.version;
app.locals.NODE_ENV = NODE_ENV;

app.use(bodyParser.urlencoded({ limit: '50mb', extended: false }));
app.use((req, res, next) => express.json({ limit: '50mb' })(req, res, next));
app.use(bodyParser.text({ limit: '50mb' }));
app.use(cookieParser());

// Used this to test a new account where meetings have more than 1 contact association
app.use("/oauth-callback", async(req, res) => {
  
  const authCodeProof = {
    grant_type: 'authorization_code',
    client_id: HUBSPOT_CID,
    client_secret: HUBSPOT_CS,
    redirect_uri: REDIRECT_URI,
    code: req.query.code
  };
  // debugger
  axios.post('/oauth/v1/token', authCodeProof)
  .then(function (response) {
    // debugger
    const tokens = response.data;
    console.log("Token: ", tokens)
    res.redirect(`/`);
  })
  .catch(function (error) {
    console.log(error);
  });
  
  
  
})

app.use("/", (req, res) => {
  res.send('<h1>Access Token & Refresh Token received</h1>');
})

// listen to connections
server.listen(PORT, () => {
  // debugger
  console.log("Server is running at PORT: ", PORT)
});
