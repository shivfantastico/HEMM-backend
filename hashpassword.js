const bcrypt = require("bcrypt");

bcrypt.hash("123321", 10).then(console.log);