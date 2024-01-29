-- ------------------------------
-- OPTION
-- ------------------------------

OPTION IMPORT;

-- ------------------------------
-- USERS
-- ------------------------------

DEFINE USER user_db ON DATABASE PASSHASH '$argon2id$v=19$m=19456,t=2,p=1$r0WZbChRcgQfo7al04w4/A$5M269E59tSqYkHWJJVZT9ebXTFejFiHtgRvs9SggYtc' ROLES VIEWER COMMENT 'test user';

-- ------------------------------
-- SCOPES
-- ------------------------------

DEFINE SCOPE user_scope SESSION 1d SIGNUP (CREATE user SET email = $email, pass = crypto::argon2::generate($pass)) SIGNIN (SELECT * FROM user WHERE email = $email AND crypto::argon2::compare(pass, $pass));

-- ------------------------------
-- TABLE: account
-- ------------------------------

DEFINE TABLE account SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TABLE: article
-- ------------------------------

DEFINE TABLE article SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TABLE: artist
-- ------------------------------

DEFINE TABLE artist SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TABLE: author
-- ------------------------------

DEFINE TABLE author SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TABLE: create
-- ------------------------------

DEFINE TABLE create SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TABLE: person
-- ------------------------------

DEFINE TABLE person SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TABLE: product
-- ------------------------------

DEFINE TABLE product SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TABLE: signin
-- ------------------------------

DEFINE TABLE signin SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TABLE: signup
-- ------------------------------

DEFINE TABLE signup SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TABLE: temperature
-- ------------------------------

DEFINE TABLE temperature SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TABLE: user
-- ------------------------------

DEFINE TABLE user SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TABLE: violet
-- ------------------------------

DEFINE TABLE violet SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TABLE: ws_article
-- ------------------------------

DEFINE TABLE ws_article SCHEMALESS PERMISSIONS NONE;

-- ------------------------------
-- TRANSACTION
-- ------------------------------

BEGIN TRANSACTION;

-- ------------------------------
-- TABLE DATA: account
-- ------------------------------

UPDATE account:j716n5mprfrl11mp48mr CONTENT { created_at: '2024-01-24T11:31:06.347880800Z', id: account:j716n5mprfrl11mp48mr, name: 'ACME Inc' };

-- ------------------------------
-- TABLE DATA: article
-- ------------------------------


-- ------------------------------
-- TABLE DATA: artist
-- ------------------------------


-- ------------------------------
-- TABLE DATA: author
-- ------------------------------

UPDATE author:john CONTENT { admin: true, age: 29, id: author:john, name: { first: 'John', full: 'John Adams', last: 'Adams' }, signup_at: '2024-01-24T10:17:45.235134Z' };
UPDATE author:lex CONTENT { admin: true, age: 42, id: author:lex, name: { first: 'Lex', full: 'Lex Draven', last: 'Draven' }, signup_at: '2024-01-24T10:48:16.218534Z' };

-- ------------------------------
-- TABLE DATA: create
-- ------------------------------


-- ------------------------------
-- TABLE DATA: person
-- ------------------------------


-- ------------------------------
-- TABLE DATA: product
-- ------------------------------


-- ------------------------------
-- TABLE DATA: signin
-- ------------------------------

UPDATE signin:sltbdv6f0d7it0z2463n CONTENT { db: 'test', id: signin:sltbdv6f0d7it0z2463n, ns: 'test', pass: 'root', user: 'root' };
UPDATE signin:wsw2jlf43fsszhtgjitr CONTENT { db: 'test', id: signin:wsw2jlf43fsszhtgjitr, ns: 'test', pass: 'root', sc: 'user_scope', user: 'root' };

-- ------------------------------
-- TABLE DATA: signup
-- ------------------------------

UPDATE signup:jtil9t25gittkevrggqv CONTENT { id: signup:jtil9t25gittkevrggqv, password: '123456', username: 'john.doe' };

-- ------------------------------
-- TABLE DATA: temperature
-- ------------------------------

UPDATE temperature:⟨018d46d7-9eff-72fc-9df0-96ac142b1459⟩ CONTENT { celsius: 37.5f, id: temperature:⟨018d46d7-9eff-72fc-9df0-96ac142b1459⟩, time: '2024-01-26T17:35:52.575768Z' };
UPDATE temperature:01HN3DF7QZNJYP899DQH2YAXWZ CONTENT { celsius: 37.5f, id: temperature:01HN3DF7QZNJYP899DQH2YAXWZ, time: '2024-01-26T17:35:52.575578Z' };
UPDATE temperature:xqy0bwas6xvi5s2ee6j2 CONTENT { celsius: 37.5f, id: temperature:xqy0bwas6xvi5s2ee6j2, time: '2024-01-26T17:35:52.572435Z' };

-- ------------------------------
-- TABLE DATA: user
-- ------------------------------

UPDATE user:16hhdf0cy3z8bgiccxlm CONTENT { id: user:16hhdf0cy3z8bgiccxlm, pass: '$argon2id$v=19$m=19456,t=2,p=1$RMCVh1ezCy/ayqOeJe1DdQ$hOuPiQeeY5rDPzwr7fsjpicamPm7jBOdFp6PKp/HKlU' };
UPDATE user:47g8tjgasgg6pw938ma1 CONTENT { id: user:47g8tjgasgg6pw938ma1, pass: '$argon2id$v=19$m=19456,t=2,p=1$n/0KapvlsOPpR893YpIORw$SALVn0nTXBHfvbc0cwkeBNAhDVkonEQA53agprQvFas' };
UPDATE user:fnr5nibvtct6jzrj20x1 CONTENT { id: user:fnr5nibvtct6jzrj20x1, pass: '$argon2id$v=19$m=19456,t=2,p=1$4Zvnn6Mnhd/ls6coUCe/LQ$I0HIRSz7lg9iCGLV2vx2fixK68sCjf7hV49WCYBgO6Q' };
UPDATE user:kwjlaq07bz56mr6340w4 CONTENT { id: user:kwjlaq07bz56mr6340w4, pass: '$argon2id$v=19$m=19456,t=2,p=1$k3EX+4dN8ofgl3PdwtjDpQ$rBnIoTcfGeyVKaRoeeDRGMjotFAE5TjKd/CB58W+W2U' };
UPDATE user:m1w4h7b89pdxny180v5v CONTENT { id: user:m1w4h7b89pdxny180v5v, pass: '$argon2id$v=19$m=19456,t=2,p=1$FwZZtU3PmcT/heuCT9YWmA$AoFJeerUPYKdYbxfiJXXyw5ZVgAMteto3cnBPgEploA' };
UPDATE user:pn4b58xjx5bhr1ou4foa CONTENT { id: user:pn4b58xjx5bhr1ou4foa, pass: '$argon2id$v=19$m=19456,t=2,p=1$QU3g5eWF4m8VLXwN9h0VxQ$HvEZRMRoqxO+10AgcTIF00F+dYmpCVUmvCk9PCXLosM' };
UPDATE user:qy0iyxlbv6vvs21wjd74 CONTENT { id: user:qy0iyxlbv6vvs21wjd74, pass: '$argon2id$v=19$m=19456,t=2,p=1$p+iBUL5CLmbRLnrcuYUQdg$Nc7ZOHND7wcyx0UAQ/qW9iQQ40g8EughYm7jzfN9U3s' };

-- ------------------------------
-- TABLE DATA: violet
-- ------------------------------

UPDATE violet:79ume790etn3ibt22933 CONTENT { accessed_date: <future> { time::now() }, id: violet:79ume790etn3ibt22933 };
UPDATE violet:bsrx1nzm3053jlr3hxvs CONTENT { id: violet:bsrx1nzm3053jlr3hxvs };

-- ------------------------------
-- TABLE DATA: ws_article
-- ------------------------------


-- ------------------------------
-- TRANSACTION
-- ------------------------------

COMMIT TRANSACTION;

