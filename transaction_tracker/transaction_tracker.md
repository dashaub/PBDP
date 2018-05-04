---
title: CSCI E-88 Final Project
author: David Shaub
geometry: margin=2cm
date: 2018-05-08
---

# Project Goal
The goal of this project is to build a pipeline that can determine the quantity of unconfirmed Bitcoin transactions that are not included in the blockchain.

Before Bitcoin transactions can enter a confirmed state and be included in a block in the chain, they enter the “mempool” on client nodes where they are candidates for inclusion. By default, nodes will eject transactions after 2 weeks if they have not been included, but since these transactions contain valid cryptographic signatures, an interested attacker might collect these unconfirmed transactions and rebroadcast them later to disrupt the network and its participants.

The goal of this project is to build a pipeline that can collect unconfirmed Bitcoin transactions and produce metrics and graphs tracking the total number of unconfirmed transactions over time so as to determine if this attack scenario is plausible.


# YouTube Demo
https://youtu.be/0pQ-KI8lsek

# Big Data Source
Three endpoints from the blockchain.info API serve as data sources. The “single block” endpoint will be used to fetch a block of confirmed transactions, the “latest block” endpoint will be used to determine the current highest block in the chain, and the “unconfirmed transactions” endpoint will be used to fetch transactions that are currently in the mempool but have not been confirmed in a block in the chain. These endpoints are described here: https://blockchain.info/api/blockchain_api

**https://blockchain.info/latestblock**: We use this before downloading blocks to determine the current height of the chain. We are only interested in the `"height"` field. An example returned looks like
```
{
    "hash":"0000000000000538200a48202ca6340e983646ca088c7618ae82d68e0c76ef5a",
    "time":1325794737,
    "block_index":841841,
    "height":160778,
    "txIndexes":[13950369,13950510,13951472]
 }
```

**https://blockchain.info/unconfirmed-transactions?format=json**: This is our source of unconfirmed transactions. A single call returns several transactions. We only need the contents of the `"hash"` field for matching unconfirmed transactions.
```
{
"txs":[

{
   "ver":2,
   "inputs":[
      {
         "sequence":4294967294,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":337127916,
            "type":0,
            "addr":"1QBhwfgwvEfhuTxaV7GKH2QAFpanxB46fA",
            "value":2970000,
            "n":21,
            "script":"76a914fe513ad1088dd81fdc78dad86b2e88512e091f3488ac"
         },
         "script":"473044022030d8789e7693dea30a29626fe820f45cba119fcabcb322e4f66cc6ee7802f6230220234981841a50ff9156250a9a9a569bffbf22eab926ed51ba88b01cbca65f8bd0012102154cf1e324689646f2e59dbb61f809a5c89f1f08d298a0eb61da9a06e418b2ae"
      },
      {
         "sequence":4294967294,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":341725582,
            "type":0,
            "addr":"1Kx1jUY5Vrzwbp5nMAAQ1fXBBi6r1U3u35",
            "value":333158,
            "n":2,
            "script":"76a914cfd9c0d9867503db8134fb51004c9314bfc069f188ac"
         },
         "script":"47304402205d941ca05c4c3125898818ece5a7e9ca446226dee750637f170549a643c343f40220429a439139911632cb387eff8ef588a69016c231b969ca0bcf4dcc33759406e501210398a21c1d327bc2b32244333a070f1e8f11a7ca2a59725466f3bcb83d14704681"
      }
   ],
   "weight":1352,
   "relayed_by":"0.0.0.0",
   "out":[
      {
         "spent":false,
         "tx_index":345969839,
         "type":0,
         "addr":"14XqwMFTpd2ypFbYuC8zPtC2BjVJ5o3tUB",
         "value":3300000,
         "n":0,
         "script":"76a91426bdb4f0dedda67890c7bc1530108acc880d215588ac"
      }
   ],
   "lock_time":521081,
   "size":338,
   "double_spend":false,
   "time":1525380136,
   "tx_index":345969839,
   "vin_sz":2,
   "hash":"49ba99e51bcaee2056ee4f5be00ab9d8c75b7df63219907087903a6b706be131",
   "vout_sz":1
},

{
   "ver":1,
   "inputs":[
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":345955296,
            "type":0,
            "addr":"1CPQZq4E4MrWQt1hvNMZj3a1yQnyut8HSh",
            "value":15783898,
            "n":0,
            "script":"76a9147ce634e9f4d211458f0cf4bd3dbd76349438067588ac"
         },
         "script":"473044022040ce0f1b7b81c7b5ed2ff52b56802225e33b207ea6c230b51246aea12c51e2cf0220018563ae929ebb1f9834c7c2f6e4a91e000624473aa813c7a4976b49a50bb7f801210344253ccc45c010bf669c2b70aac460ebecefaa0130e5a435fd8561df99d744f9"
      },
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":345812889,
            "type":0,
            "addr":"1MamDaLQKuXB4nwV4ShXZ7Zy2M9uwvWAaT",
            "value":13000000,
            "n":0,
            "script":"76a914e1c5271a4fe8d9144fea01d4927368c8851428a288ac"
         },
         "script":"48304502210093806b14b7ad0b1794b2c46c0e83724072952b4ef61ea7dc5d1dc4995f94d08e022048e8c842cdd5e8a2a1860d9e2562a8ecd37768c8a4692a48c3cb07095ce017c601210337c92905cb392e749f085d2775f741f7ac3790088a5824b09377e31ebfbcdcbe"
      }
   ],
   "weight":1484,
   "relayed_by":"127.0.0.1",
   "out":[
      {
         "spent":false,
         "tx_index":345969840,
         "type":0,
         "addr":"1PsqxvYrCL2h8TjnH9TSN3agNnybbEi7yo",
         "value":11163800,
         "n":0,
         "script":"76a914faf0704701ce0dcc3f357f3143045425a0061edd88ac"
      },
      {
         "spent":false,
         "tx_index":345969840,
         "type":0,
         "addr":"3CE1wyzeUUJSthvkwM7Vauuu6upVqF3FD2",
         "value":17610000,
         "n":1,
         "script":"a914738e2c62402879e45ab38bc9c2098f8a0a05032f87"
      }
   ],
   "lock_time":0,
   "size":371,
   "double_spend":false,
   "time":1525380136,
   "tx_index":345969840,
   "vin_sz":2,
   "hash":"8b4dc4a85ac03024c2a76793b32011a68d3ceb949965a6f0db5e469ff989ee82",
   "vout_sz":2
},

{
   "ver":1,
   "inputs":[
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":345863552,
            "type":0,
            "addr":"1CMSxFdw9i4bXnpRtdxdkwGd5sg2hYMT3",
            "value":2386000,
            "n":1,
            "script":"76a9140225a4ee279b7dfd5472df1f450f61f4bd9faa1788ac"
         },
         "script":"47304402202c44666450564ceb820a168ea34a14bb2e1ea4df5999955d49b79f3871a9257702201ace8c10ed96d5e686a86f15198475d17072381f0c79c51c8ce645dd03d69cbd012103a5a783b627c4a9cee9d17d05d06bcfaac3f89ab88a3f70064bc9ca396c02bd23"
      }
   ],
   "weight":900,
   "relayed_by":"0.0.0.0",
   "out":[
      {
         "spent":false,
         "tx_index":345969065,
         "type":0,
         "addr":"1Cwu7e1AtdQBtNHue1FosTHfvSZ56LuEB3",
         "value":125098,
         "n":0,
         "script":"76a914830b5937db086ff9b056a539912dd9d1deb5b25888ac"
      },
      {
         "spent":false,
         "tx_index":345969065,
         "type":0,
         "addr":"12NPv7DVB6BVnBpC93VToCdUeKgnSymh1W",
         "value":2258600,
         "n":1,
         "script":"76a9140f03fd704dbb6ebf8b2565c8dc6baea86e20f21488ac"
      }
   ],
   "lock_time":0,
   "size":225,
   "double_spend":false,
   "time":1525380136,
   "tx_index":345969065,
   "vin_sz":1,
   "hash":"58797ddc3a3a9a5b6541ab02a9ba05969bf2afe5d806495c3193121701f3bda4",
   "vout_sz":2
},

{
   "ver":2,
   "inputs":[
      {
         "sequence":4294967293,
         "witness":"0248304502210096e890dc9930592ff7b26b0ced76f1d96f4e21d9175f1c768a5579e087ed224f02204aa5d9ebdcadea478306aaa8aa25af5b6f7a249297b6fd4c930c308e38e9612d0121037078115b5498a201d037222b3fb7fa7cacc6c928d6d9aa09a33ce54a9ebdf9c3",
         "prev_out":{
            "spent":true,
            "tx_index":345968077,
            "type":0,
            "addr":"3QQCrtaw1bUhiQAKGDS2YD2rgiajHmcsgZ",
            "value":6472373,
            "n":0,
            "script":"a914f91cf390f9f462e0bec5d6229055cea01f80cd7a87"
         },
         "script":"1600147b4695382e8417614bb675d439b7edd93ab5290a"
      }
   ],
   "weight":662,
   "relayed_by":"0.0.0.0",
   "out":[
      {
         "spent":false,
         "tx_index":345969066,
         "type":0,
         "addr":"34jYBzyMyDMWGizhBhmtbcniVAN5yK9sGt",
         "value":5848041,
         "n":0,
         "script":"a91421627976e8be07cc2a351eec7d2707ed6f8df71787"
      },
      {
         "spent":false,
         "tx_index":345969066,
         "type":0,
         "addr":"3PNimW16wcwF3hQrg482QVwfnub4c87XV5",
         "value":621000,
         "n":1,
         "script":"a914eddcf57a3f93598c9f7ec86ea9bf580117d8e93187"
      }
   ],
   "lock_time":521081,
   "size":248,
   "rbf":true,
   "double_spend":false,
   "time":1525380137,
   "tx_index":345969066,
   "vin_sz":1,
   "hash":"fbbf32186b17832f9412e7c95e77d430904aec3abb87ac376207ce94c10ff88d",
   "vout_sz":2
},

{
   "ver":1,
   "inputs":[
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":345969762,
            "type":0,
            "addr":"1LWcXZ2smbnBeeHHnKKDE2kjNbB7kiaCgq",
            "value":127543722,
            "n":1,
            "script":"76a914d6041be98b0989bea9d252c5ec1db7013e2738af88ac"
         },
         "script":"47304402201806de7af2dff7fbbd1fec8ea66f327e5397549f0750c3cbeae0fce6904a150502200d438c765e95513c80ecc22aeb71e16ba03b4b093b8cde2c9854dd26c97adb5b01210367fa21d37e08e09d2c46f6e50814fef4b3731d5d6d149cf65ce1e33a79a7346e"
      }
   ],
   "weight":900,
   "relayed_by":"127.0.0.1",
   "out":[
      {
         "spent":false,
         "tx_index":345969841,
         "type":0,
         "addr":"1CfjkuSt21uPFPAYAkVKUyTsNuRmQ3sLWY",
         "value":2020312,
         "n":0,
         "script":"76a9147ffce1c392c1c4199705f0c6c195623202cef3d188ac"
      },
      {
         "spent":false,
         "tx_index":345969841,
         "type":0,
         "addr":"16P1RkLyjdCsgwBBqmcq3xFfjPsk5Ma8KU",
         "value":125522054,
         "n":1,
         "script":"76a9143b0222e158e13d871498c8f93564bbced3b5441f88ac"
      }
   ],
   "lock_time":0,
   "size":225,
   "double_spend":false,
   "time":1525380137,
   "tx_index":345969841,
   "vin_sz":1,
   "hash":"c33025727b9d496abd198717c59dfa38363ed0773db271e3c183f14a7d43a639",
   "vout_sz":2
},

{
   "ver":1,
   "inputs":[
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":345368661,
            "type":0,
            "addr":"13gi3FMUHnZYv7d94Q9HQopty8CpaKrGvk",
            "value":1100000,
            "n":0,
            "script":"76a9141d72bb4397bf922a0285889e7188c8b9b22af3e688ac"
         },
         "script":"4830450221008c95607015eb3081bb0b378f51e933d359d6938bc3459bb50321b062262fad0202203a6131f0372ac52bf02d303f52c82dbb55909974ebf2ed41459673f40514084f012103ecd73aecacd19408fd1816b95f022ba8308f51067cab1d98a89e9482f4995800"
      },
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":340422023,
            "type":0,
            "addr":"1GPaurX3yWQ4T7EVdbyGjmkmec6UbBAFi9",
            "value":229378,
            "n":0,
            "script":"76a914a8cf587a8f384d866259391bf0d6d38d938a10c688ac"
         },
         "script":"47304402205bd2660c9e08ade3f73d88756c2eebe2f139efee53745c91d1891204a94a9332022072cf6bd65f1c59ed0724052e25df9dc017b804b7796e220dcfce4cffc7b26258012102e88b6debe14f2dce7b4523ff1e1bae524c6c3960fdc860caffb7720ca7b8d3cc"
      },
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":333503847,
            "type":0,
            "addr":"16Vr2RfsYx63MJaGpinD8nkKLNL3NzA1JT",
            "value":262840,
            "n":0,
            "script":"76a9143c4d3312a57c4604c8f1a10e1fceb7ffbc8ca54d88ac"
         },
         "script":"47304402201c3ba4e08c404c32ca32b41d860c24abee3ed4d370b0cc9c1fdf8b44ac01531502205f97d8a38bc4db5fee3576d7b60b3d3bd3850f56dd682b1ef3862f447dd54c370121022854a0dcae75ba22667b912f035c9eb84e6549709013f9d5f5452dea67b34222"
      },
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":308926385,
            "type":0,
            "addr":"1BJbPwaPzeNVm8ayqgUniLyXsSyVA5574A",
            "value":249181,
            "n":0,
            "script":"76a91471050a87c08191f48c9e208a0c31e06a562f140488ac"
         },
         "script":"48304502210089442b1f719a0925e9d0f9de0effae7328591883d68d7400db1508479b0f9a8302207af28bf0be9a2a6a8224e3e6bc64c7878b63cb139674ca706dc913b411b0a816012103057f7fa2b8834a0fc4905e489d54c108ef9b4c55912417df2cae7c896560ee4c"
      },
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":341085533,
            "type":0,
            "addr":"1Cd15zJKFhxbc7nDuWuAuiwMvUKNn9TwWB",
            "value":264487,
            "n":0,
            "script":"76a9147f786e16b1dcba9ba84350dc9078f8fe7b5fcd9288ac"
         },
         "script":"483045022100c756f70432f56917ac3b559afe10fb9fbda2fa5e28805c204ecfa6bd8560b49302206f4bad101fcddea12544cbf304147a75e0761ee2716f5d90c3a07a5e0e42a214012103f1619356e854b340bb20709bed44c7ee642f5321cbf1a9511ef2b1ea7ed896da"
      },
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":314515259,
            "type":0,
            "addr":"1Pw5rzDdwGdDtEpiaccYUg44kp1ZUKzB4g",
            "value":219150,
            "n":0,
            "script":"76a914fb8d49a28a7e4cd94f2be90073861b648a24e74988ac"
         },
         "script":"4730440220046a6865e5579c22004fd078ff626a4e6eb1a929a8ca6928cd9b5452a4139d8402205fed1ae2adc1896a23e439b80d884a9d973fe36f1ae939daac09379b98ea72fd012102374b342e35d48b2c8608fed6d44b0f5c9b783751da9bbe131011e4a9f84c7314"
      },
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":338337180,
            "type":0,
            "addr":"1AN89DRbMV4ZhGWjd8FWxATfBFtk9AvjVx",
            "value":235903,
            "n":0,
            "script":"76a91466b7d42ae10ef7789e412dfa980d08487e97497788ac"
         },
         "script":"483045022100c582a02e9b30e7b975042ab624e5c18efc79c8953e8b8b5fc55902a1cff1b9020220321527e052413b02ca9fd261b5b04bc77017d2b2939f2c0c2d7bb12c6b66ac500121023171a530f2037edc48ce2051600c969a7568519186269099807647981582c926"
      },
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":333871802,
            "type":0,
            "addr":"1FQRY1VokbDDvTFoywVcH68AvDvMyeAdxz",
            "value":301041,
            "n":0,
            "script":"76a9149dffcefe94deece96b3f63f448c9efbc74bb225588ac"
         },
         "script":"4830450221009982f5d0901f57585d56872c62029228b615dc323c2e251e4fa43dc291e20d2102200e96b5fe264c4875d9aaf3ec12f9c0a7349614f4e34ff62f9ef7460d268a2fe2012103fc2ae4c7a77dc13de2b455fc8b2e868504950f47404f9d820fb82ce4ba64b6c5"
      },
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":331654401,
            "type":0,
            "addr":"18pJqj59MGpEbVe9r1KSKyGDD2pd3P7LLu",
            "value":214016,
            "n":0,
            "script":"76a91455bb52a649e30549da4fa1f6af377a1d041f3f3a88ac"
         },
         "script":"4830450221009ab755e3af1c2b055ae862daf82039d0d6c1f3f556792237eb3edcac3fb6722202204a45381a412cd7c21f5b12aa4a734dd0d6e443ffca2bf3d4de601e36bbdd3e32012103848a1d521e291daf86e7f1d8a3247f66932b13b2d834f1c09c8a4aa7a4f1669e"
      },
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":321642833,
            "type":0,
            "addr":"1Mi4WhJsHYqiiRVSq8CfRccf6z9CYo7HeA",
            "value":253617,
            "n":1,
            "script":"76a914e32680739afc9d782d0c079cd7ce4e6fbc81683188ac"
         },
         "script":"4730440220764007edf45155a63a2e228ca1089902d3ed94133518a719299c16e89f332af8022020d442fc7f9500849e63ae2a8924956987b9a249f78e7353e9a639bf3a0bd3df012103f71bfca003a6fad0f32f77fd2140cc010e83e5784b56ff05874049126429b9c9"
      }
   ],
   "weight":6216,
   "relayed_by":"127.0.0.1",
   "out":[
      {
         "spent":false,
         "tx_index":345969842,
         "type":0,
         "addr":"189hQxcX6GkiW1iHFdR44ZbC3S9rnfkEe",
         "value":205965,
         "n":0,
         "script":"76a914015a2be3452c06ecc565b0c7c63b069c462892ab88ac"
      },
      {
         "spent":false,
         "tx_index":345969842,
         "type":0,
         "addr":"18CDuo4pGUFtodK1EyjFVK9SEBGLditfEs",
         "value":3114300,
         "n":1,
         "script":"76a9144ee83ba5a5aa969e07ae5755b298c8f7a883a2a988ac"
      }
   ],
   "lock_time":0,
   "size":1554,
   "double_spend":false,
   "time":1525380139,
   "tx_index":345969842,
   "vin_sz":10,
   "hash":"f94daf96b8507c26f21ae2ec91e00f5d71d947511d5faddd9d9ff6f919ceb905",
   "vout_sz":2
},

{
   "ver":1,
   "inputs":[
      {
         "sequence":4294967295,
         "witness":"02473044022056ffdf3d01ba2dcca868afdf4283270ced38518f03cc040d849c3df31c7bae3f02203d4f4da1e3d35915b96e00992294664d67b70194ec81e64afcfe3bc04efd2314012103fa000bc348dec689a5bc4c82d5a6d17c29e43fca79bbd82a26587b9bed8ea857",
         "prev_out":{
            "spent":true,
            "tx_index":345961398,
            "type":0,
            "addr":"39dpCTjGNPkhhorS8yyvrbnuJgHpTwmJMa",
            "value":300000,
            "n":52,
            "script":"a9145725f86b340bc82fcf9a04b5f6c3af0bf186342987"
         },
         "script":"16001437a4ac2252347019b92848aeb16782a3e1b05ace"
      }
   ],
   "weight":665,
   "relayed_by":"0.0.0.0",
   "out":[
      {
         "spent":false,
         "tx_index":345969843,
         "type":0,
         "addr":"1F3Cx8cLh94eVCfiGyAzwHFVkxNcnfrWP3",
         "value":283475,
         "n":0,
         "script":"76a91499fc919686611d58429a1f411f01c1ab0ed0a35088ac"
      },
      {
         "spent":false,
         "tx_index":345969843,
         "type":0,
         "addr":"bc1qgu5wp0m83h255zd57ef852l3zlzrjdye75q6cy",
         "value":13172,
         "n":1,
         "script":"00144728e0bf678dd54a09b4f6527a2bf117c4393499"
      }
   ],
   "lock_time":0,
   "size":248,
   "double_spend":false,
   "time":1525380140,
   "tx_index":345969843,
   "vin_sz":1,
   "hash":"e45e6b023bf4a45c25a4b57cb7163d491e2c93d664ae8afd4adb656b3b78b697",
   "vout_sz":2
},

{
   "ver":1,
   "inputs":[
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":345968985,
            "type":0,
            "addr":"1DJ4ggHQwMdjoYE7m7uZS3bvY22gjtvy2x",
            "value":845117,
            "n":1,
            "script":"76a91486dba7f4e0288eda9dd744ec4dccff395d5839de88ac"
         },
         "script":"47304402206ea1a15338c1dc4682b0a86ff38b063d6ca632ebaf6d636a2d31ec9df08acf94022024a39c7a826d50b99192889c0edfca6bd9db34b5b31b409b7979b2a1f876b45d0121021ee598df96ec793bbc780ca8d93837f2e3ba39e206790a97010f45f78d69eccc"
      }
   ],
   "weight":900,
   "relayed_by":"127.0.0.1",
   "out":[
      {
         "spent":false,
         "tx_index":345969067,
         "type":0,
         "addr":"16JrcZoNji37qDvCKggFEFLzk8sEKo2Bmw",
         "value":2495,
         "n":0,
         "script":"76a9143a391d831a562e50d7e5bba966c7be7d9b6eb98588ac"
      },
      {
         "spent":false,
         "tx_index":345969067,
         "type":0,
         "addr":"1ABor9WhdvxMeQdgxcBaqURs9y6nGutQbC",
         "value":841266,
         "n":1,
         "script":"76a91464c465db42fc092482cf44988bb2f99ae9258fe088ac"
      }
   ],
   "lock_time":0,
   "size":225,
   "double_spend":false,
   "time":1525380140,
   "tx_index":345969067,
   "vin_sz":1,
   "hash":"9aabfd74152664fef3c76d2747231e34fb53fb0337b2cf33a37882036c084469",
   "vout_sz":2
},

{
   "ver":1,
   "inputs":[
      {
         "sequence":4294967295,
         "witness":"024830450221009ff0b4470ce27043b8bf01b77c0c38931f17cfea87cc6a3f2948ab5ca52fe10c022000c10aa22d5fcea81f51547708ae3e2e448d32ec71cc8a0b99ef1029bdcc6849012102bf59522bda75db1f3233b55d1d06f9dae578de4db7ec572d6985f87a1314c503",
         "prev_out":{
            "spent":true,
            "tx_index":345922758,
            "type":0,
            "addr":"bc1qg74pfz7rx0a79e4achnkggytcd7u3zl28cp600",
            "value":83713,
            "n":0,
            "script":"001447aa148bc333fbe2e6bdc5e764208bc37dc88bea"
         },
         "script":""
      }
   ],
   "weight":574,
   "relayed_by":"0.0.0.0",
   "out":[
      {
         "spent":false,
         "tx_index":345969068,
         "type":0,
         "addr":"bc1qganzjde4vtxmt7a902qftu2mqmc3m93utffqdq",
         "value":541,
         "n":0,
         "script":"0014476629373562cdb5fba57a8095f15b06f11d963c"
      },
      {
         "spent":false,
         "tx_index":345969068,
         "type":0,
         "addr":"1hjc5gYVjCm9gaFZZzb3ToLwQtUA8otqQ",
         "value":80281,
         "n":1,
         "script":"76a91407b43404fab9e126dca8baa14f89f37cf3b75ad888ac"
      }
   ],
   "lock_time":0,
   "size":226,
   "double_spend":false,
   "time":1525380140,
   "tx_index":345969068,
   "vin_sz":1,
   "hash":"208e8c4ef6a0ddec9ebfeae69a603bc44cb3ca9ded8d83e2aad583aaa03c8ed5",
   "vout_sz":2
},

{
   "ver":1,
   "inputs":[
      {
         "sequence":4294967295,
         "witness":"",
         "prev_out":{
            "spent":true,
            "tx_index":345775959,
            "type":0,
            "addr":"1JJ7yzWe6uBPhHR6UpnukD6XFxfWaSr36Z",
            "value":160260,
            "n":1842,
            "script":"76a914bdb709d8f3665f94c13bd011da2e9970cf725f5688ac"
         },
         "script":"4830450221009fe3c4334ea7b4bf7ea8ea94ddc6e9d2aa7a2b79f27f3e83158c79d5f4350a9f02200fe229c58539b1f6b965851a85bbbe17f52c4c13128362cf33dd06e6dab64a9b0121021176042b85e10ac6be2bc3a451f03292ffdd9007c3796f3382262a9ad39dd440"
      }
   ],
   "weight":892,
   "relayed_by":"0.0.0.0",
   "out":[
      {
         "spent":false,
         "tx_index":345969844,
         "type":0,
         "addr":"bc1qs4v2clwx7ev2xtuhvgzmtvfr526g09ehy7wgvu",
         "value":783,
         "n":0,
         "script":"00148558ac7dc6f658a32f976205b5b123a2b4879737"
      },
      {
         "spent":false,
         "tx_index":345969844,
         "type":0,
         "addr":"15CkBxvojswus8hmK3XMkQHUPShWG6B21D",
         "value":155000,
         "n":1,
         "script":"76a9142e1921905d731d7abd14d530630d3f7110d618e188ac"
      }
   ],
   "lock_time":0,
   "size":223,
   "double_spend":false,
   "time":1525380140,
   "tx_index":345969844,
   "vin_sz":1,
   "hash":"288dff44b180797fe3ff19588c21274d272f8961ea28d47756f26400aa3f2003",
   "vout_sz":2
}]
}
```

**https://blockchain.info/block-height/$block_height?format=json**: We fetch blocks at specified height here for searching for confirmed transactions. Once again, we only need the `"hash"` field.
```



{ "blocks" : [


{
    "hash":"000000007bc154e0fa7ea32218a72fe2c1bb9f86cf8c9ebf9a715ed27fdb229a",
    "ver":1,
    "prev_block":"00000000cd9b12643e6854cb25939b39cd7a1ad0af31a9bd8b2efe67854b1995",
    "mrkl_root":"2d05f0c9c3e1c226e63b5fac240137687544cf631cd616fd34fd188fc9020866",
    "time":1231660825,
    "bits":486604799,
    "fee":0,
    "nonce":1573057331,
    "n_tx":1,
    "size":215,
    "block_index":14949,
    "main_chain":true,
    "height":100,
    "tx":[

{
   "lock_time":0,
   "ver":1,
   "size":134,
   "inputs":[
      {
         "sequence":4294967295,
         "witness":"",
         "script":"04ffff001d014d"
      }
   ],
   "weight":536,
   "time":1231660825,
   "tx_index":14958,
   "vin_sz":1,
   "hash":"2d05f0c9c3e1c226e63b5fac240137687544cf631cd616fd34fd188fc9020866",
   "vout_sz":1,
   "relayed_by":"0.0.0.0",
   "out":[
      {
         "spent":false,
         "tx_index":14958,
         "type":0,
         "addr":"13A1W4jLPP75pzvn2qJ5KyyqG3qPSpb9jM",
         "value":5000000000,
         "n":0,
         "script":"4104e70a02f5af48a1989bf630d92523c9d14c45c75f7d1b998e962bff6ff9995fc5bdb44f1793b37495d80324acba7c8f537caaf8432b8d47987313060cc82d8a93ac"
      }
   ]
}]
 }

]}
```

If this project were being built with the goal of deploying for enterprise use, rather than using the blockchain.info API, we should instead run our own `bitcoind` node (or even several nodes) and collect the statistics on latest block, confirmed transactions, and unconfirmed transactions there. This would allow us to avoid being rate-limited by the API, collect event streams with RPC or by grepping the local logs, and give us custom control on the rebroadcast and retention polices of our node’s mempool. However, running a node requires ~150GB disk space and a significant amount of time for the initial sync—perhaps exceeding a week. Therefore, we should view this project as a proof of concept with the idea that the input data sources and collection tier could be swapped later, but the rest of the data pipeline would not require modifications if we had sufficient hardware to support the full quantity of data.



# Expected Results
There will be two main outputs of interest, one built upon the other. Every five minutes that the Apache Beam job runs, a count of the current cumulative number of unconfirmed transactions will be recorded in InfluxDB. This data will be used to create a timeseries plot in Grafana. The Grafana dashboard is the main output and allows us to visualize trends in the total number of unconfirmed transactions. The batch tier views that are stored in InfluxDB could be interesting on their own (e.g. we might build a forecasting model to predict the number of unconfirmed transactions at some future time), but for this pipeline we will limit the use to the Grafana visualization.

While we might expect that the cumulative number of unconfirmed transactions will necessarily increase over time, this is not necessarily guaranteed: users might submit few transactions, and old unconfirmed transactions might be included in new blocks, decreasing the total number of unconfirmed transactions available. Alternatively, while the default node mempool policy ejects transactions after 2 weeks, individuals node operators are free to select their own settings or even rebroadcast old transactions. Finally, a massive surge in usage could naturally lead to a growing mempool. Existing websites let users track the current mempool (e.g. https://jochen-hoenicke.de/queue/#1,all), but these sites do not track the current cumulative unconfirmed transactions as this project does.


# Processing Pipeline

![](architecture.png)

In the Bitcoin network, blocks are produced (on average) every 10 minutes, so while creating a streaming pipeline could be interesting, we can produce a close enough approximation by running a batch job that runs every 5 minutes and accepting that our result could be up to five minutes stale if a block were produced right after a run occurs. Our Python script will check for updates to the mempool with high frequency (e.g. once every second), but the script that follows the blockchain can check every five minutes and then make subsequent follow-up fetches if it has missed a block during that time.

The Apache Beam job will perform deduplication of the events entering from each source. Furthermore, it will only output transactions that appear in the unconfirmed source but not in the confirmed source. After these results are written to files, a Python job will pick up the results and insert a job timestamp and the count of unconfirmed transactions as a batch tier view into InfluxDB. Finally, Grafana can utilize the InfluxDB data source to display graphs with various time aggregation periods (e.g. hourly, daily, weekly, etc).

**Collection tier**: Python scripts using the “blockchain” Python module to fetch data from the blockchain.info API. The results will be written to filesystem.

**Batch tier**: Apache Beam for joining the confirmed and unconfirmed transactions to determine which transactions are still unconfirmed. Alternatively, we could use Spark or Flink here. Beam is still a fairly new technology, and unfortunately the Python support is limited: no database connectors exist, so we will be reading and writing to filesystem.

**Batch view storage**: InfluxDB for collecting timestamps and the current count of cumulative unconfirmed transactions. A Python job will insert this after the Beam batch job runs. InfluxDB is very well suited for collecting high volume timeseries data (such as IoT measurements or system load), and our single count inserted every 5 minutes is very low load for it.

**Visualization tier**: Grafana dashboard displaying a timeseries plot of current, cumulative unconfirmed transaction counts.


# Implementation

Once we launch our container, the job starts with the `run_pipeline.sh` script.
```
#!/bin/bash

set -euo pipefail
cd /root/transaction_tracker/

# Start the InfluxDB and Grafana service
service influxdb start
service grafana-server start

# Start collecting unconfirmed transactions
python fetch_mempool.py &

# Collect some events in the mempool before following the chain
sleep 120
python follow_chain.py &

echo "drop database blockchain;" | influx
echo "create database blockchain;" | influx
while :; do
    # Launch a Beam pipeline run every 5 minutes
    sleep 300
    timestamp=$(date -u '+%Y-%m-%dT%H%M%SZ')
    echo "Launching batch job at ${timestamp}"
    python beam_pipeline.py --timestamp="${timestamp}"

    # Insert the results in InfluxDB
    python influx_insert.py --timestamp="${timestamp}"
done

```

As part of the collection tier, `fetch_mempool.py` will run and collect unconfirmed transactions from blockchain.info that are written to filesystem in batches.
```
"""
Fetch the current mempool
"""
SLEEP_TIME=2

from blockchain import blockexplorer
import time
import uuid
import argparse

def read_api_key():
    """
    Read the api key from the file
    """
    try:
        with open('api.key') as api:
            return api.readline().strip()
    except:
        return None

api_key = read_api_key()
parser = argparse.ArgumentParser()
parser.add_argument('--block_height', help='Get the block at the specified height',
                    type=int, required=False)
args = parser.parse_args()


def write_transactions(transactions):
    """
    Write transactions to file
    """
    output_file = 'unconfirmed/{}.txt'.format(uuid.uuid4().hex)
    with open(output_file, 'w') as output:
        for line in transactions:
            output.write(line + '\n')

def get_mempool():
    """
    Return a list of transaction hashes currently in the mempool
    """
    # Protect against API timeout with very crude try-catch
    try:
        unconfirmed = blockexplorer.get_unconfirmed_tx()
        transactions = ['{} unconfirmed'.format(tx.hash) for tx in unconfirmed]
    except:
        return None
    return transactions

print 'Using api key: ' + api_key
while True:
    transactions = set()
    # Collect batches together before writing results
    for _ in range(60 / SLEEP_TIME):
        current_transactions = get_mempool()
        # Prevent duplicates within one batch
        for transaction in current_transactions:
            transactions.add(transaction)
        time.sleep(SLEEP_TIME)
    transactions = list(transactions)
    print 'Writing {} transactions'.format(len(transactions))
    write_transactions(transactions)

```

We also laucnh the `follow_chain.py` in the collection tier to collect confirmed transactions and write these to the filesystem.
```
"""
Follow the chain, download blocks, and extract transactions
"""
from blockchain import blockexplorer

import time
import uuid


def read_api_key():
    """
    Read the api key from the file. An API key is not absolutely required for the application
    to run, but it can help prevent request throttling and support higher query frequency.
    """
    try:
        with open('api.key') as api:
            return api.readline().strip()
    except:
        return None

api_key = read_api_key()
processed_heights = set()
best_height = blockexplorer.get_latest_block(api_code=api_key).height - 1


def process_block(block_height):
    """
    Fetch a block and write the results
    :param block_height: The chain height
    """
    print 'Processing new block at height {}'.format(block_height)
    transactions = fetch_block(block_height)
    write_block(transactions, block_height)
    processed_heights.add(block_height)

def fetch_block(block_height):
    """
    Fetches a block at a specified chain height and returns a list of its transactions
    :param block_height: The chain height
    """
    block = blockexplorer.get_block_height(height=block_height, api_code=api_key)[0]
    transactions = ['{} {}'.format(tx.hash, block.height) for tx in block.transactions]
    return transactions

def write_block(transactions, height):
    """
    Write a list of transactions to file
    :param transactions: A list of transaction hashes
    :param height: The block height
    """
    output_file = 'blocks/{}_{}.txt'.format(height, uuid.uuid4().hex)
    with open(output_file, 'w') as output:
        for line in transactions:
            output.write(line + '\n')

print 'Using api key: ' + api_key
while True:
    # Determine current best block
    current_height = blockexplorer.get_latest_block(api_code=api_key).height
    # Only process a block if it is better than current best block
    if current_height > best_height:
        process_block(current_height)
        # Process any other blocks we may have missed
        # more than 10 missed blocks in 1 minute occurs with probability 2.285845e-19
        for candidate_height in range(current_height - 10, current_height):
            if candidate_height not in processed_heights:
                process_block(candidate_height)
        best_height = current_height


    # Wait 5 minutes before checking for a new block
    time.sleep(60 * 5)

```

Now that we have collected the necessary data to determine if a transaction is still unconfirmed, we can launch an Apache Beam job in `beam_pipeline.py`.
```
import argparse
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--timestamp', type=str, help = 'Current run timestamp',
                    required = True)
args = parser.parse_args()
timestamp = args.timestamp

print 'Launching Beam job {}'.format(timestamp)

p = beam.Pipeline(options=PipelineOptions())

confirmed = (p | 'readConfirmed' >> beam.io.ReadFromText('blocks/*.txt')
               | 'tupleConfirmed' >> beam.Map(lambda x: x.strip().split(' ')))
unconfirmed = (p | 'readUnconfirmed' >> beam.io.ReadFromText('unconfirmed/*.txt')
                 | 'tupleUnconfirmed' >> beam.Map(lambda x: x.strip().split(' ')))


results = {'confirmed': confirmed, 'unconfirmed': unconfirmed} | beam.CoGroupByKey()
def filter_unconfirmed(x):
    """
    Extract the transaction id for transactions that have not been confirmed
    """
    if len(x[1]['unconfirmed']) > 0 and len(x[1]['confirmed']) == 0:
        yield x[0]

filtered = results |'filterTuples' >> beam.ParDo(filter_unconfirmed)

filtered | 'write_output' >> beam.io.WriteToText('beam/{}_results'.format(timestamp))
result = p.run()
result.wait_until_finish()

```

InfluxDB requires no additional configuration: the `run_pipeline.sh` script that we launch inside our Docker container already created the database for us. We do need to write the filesystem results from the Apache Beam job into InfluxDB, however, so `influx_insert.py` does this.
```
import argparse
import glob
from influxdb import InfluxDBClient

parser = argparse.ArgumentParser(description='Process some integers.')
parser.add_argument('--timestamp', type=str, help = 'Current run timestamp',
                    required = True)
args = parser.parse_args()
timestamp = args.timestamp

print 'Launching InfluxDB insert for job {}'.format(timestamp)

client = InfluxDBClient(database='blockchain')
#timestamp = '2018-04-23T193341Z'
def insert_influx(timestamp, count):
     """
     Insert the count into InfluxDB
     :param filename: The file to process
     """
     json_body = [{"measurement": "unconfirmed_transactions",
                   "time": timestamp,
                   "fields": {"count": count}
                   }]
     client.write_points(json_body)


def count_lines(filename):
     """
     Count the number of transactions in a file
     :param filename: The file to process
     """
     return sum(1 for line in open(filename))

# Safety if multiple output files
filenames = glob.glob('beam/{}*'.format(timestamp))

count = 0
# Insert total number of unconfirmed transactions into InfluxDB
for filename in filenames:
     count += count_lines(filename)
insert_influx(timestamp, count)

print 'InfluxDB insert complete for job {}'.format(timestamp)

```

The configuration for the configured Grafana dashboard in `grafana_dashboard.json` that we exported:
```
{
  "annotations": {
    "list": [
      {
        "builtIn": 1,
        "datasource": "-- Grafana --",
        "enable": true,
        "hide": true,
        "iconColor": "rgba(0, 211, 255, 1)",
        "name": "Annotations & Alerts",
        "type": "dashboard"
      }
    ]
  },
  "editable": true,
  "gnetId": null,
  "graphTooltip": 0,
  "id": 1,
  "links": [],
  "panels": [
    {
      "aliasColors": {},
      "bars": false,
      "dashLength": 10,
      "dashes": false,
      "datasource": null,
      "fill": 2,
      "gridPos": {
        "h": 8,
        "w": 24,
        "x": 0,
        "y": 0
      },
      "id": 2,
      "legend": {
        "avg": false,
        "current": false,
        "max": false,
        "min": false,
        "show": true,
        "total": false,
        "values": false
      },
      "lines": true,
      "linewidth": 2,
      "links": [],
      "nullPointMode": "null",
      "percentage": false,
      "pointradius": 2,
      "points": true,
      "renderer": "flot",
      "seriesOverrides": [],
      "spaceLength": 10,
      "stack": false,
      "steppedLine": false,
      "targets": [
        {
          "alias": "Cumulative Unconfirmed Transactions",
          "groupBy": [
            {
              "params": [
                "$__interval"
              ],
              "type": "time"
            },
            {
              "params": [
                "null"
              ],
              "type": "fill"
            }
          ],
          "measurement": "unconfirmed_transactions",
          "orderByTime": "ASC",
          "policy": "autogen",
          "refId": "A",
          "resultFormat": "time_series",
          "select": [
            [
              {
                "params": [
                  "count"
                ],
                "type": "field"
              },
              {
                "params": [],
                "type": "mean"
              }
            ]
          ],
          "tags": []
        }
      ],
      "thresholds": [],
      "timeFrom": null,
      "timeShift": null,
      "title": "Cumulative Unconfirmed Transactions",
      "tooltip": {
        "shared": true,
        "sort": 0,
        "value_type": "individual"
      },
      "type": "graph",
      "xaxis": {
        "buckets": null,
        "mode": "time",
        "name": null,
        "show": true,
        "values": []
      },
      "yaxes": [
        {
          "format": "short",
          "label": null,
          "logBase": 1,
          "max": null,
          "min": null,
          "show": true
        },
        {
          "format": "short",
          "label": null,
          "logBase": 1,
          "max": null,
          "min": null,
          "show": true
        }
      ]
    }
  ],
  "refresh": false,
  "schemaVersion": 16,
  "style": "dark",
  "tags": [],
  "templating": {
    "list": []
  },
  "time": {
    "from": "now-30m",
    "to": "now"
  },
  "timepicker": {
    "refresh_intervals": [
      "10s",
      "1m",
      "5m",
      "15m",
      "30m",
      "1h",
      "2h",
      "1d"
    ],
    "time_options": [
      "5m",
      "15m",
      "1h",
      "6h",
      "12h",
      "24h",
      "2d",
      "7d",
      "30d"
    ]
  },
  "timezone": "",
  "title": "Unconfirmed Transactions",
  "uid": "gVL19zMmk",
  "version": 5
}

```

# Results

Everything needed to run the project is contained inside `transaction_tracker.tar.gz`. The only requirements are Docker, an active internet connection to download the packages for the Docker container and to connect to the Blockchain.info API, and a browser to view the Grafana UI.

## Build the project

From the top-level directory with `Dockerfile`, launch:
```
$ docker build . --compress -t transaction_tracker:1.0
```
This builds a Docker image from Ubuntu 18.04 with Python (and modules for Beam, InfluxDB, and the blockchain.info API), Apache Beam, InfluxDB, and Grafana installed.

## Launch the container

Once the build success, run the container:
```
$ docker run --init -p 3000:3000 -p 8086:8086 -ti transaction_tracker:1.0 /root/transaction_tracker/run_pipeline.sh
```

You should start to see output like
```
Starting influxdb...
influxdb process was started [ OK ]
 * Starting Grafana Server                                                                                                                                                                           [ OK ] 
Using api key: <REDACTED>
Writing 195 transactions
Using api key: <REDACTED>
Processing new block at height 519900
Processing new block at height 519890
Processing new block at height 519891
Processing new block at height 519892
Processing new block at height 519893
Processing new block at height 519894
Processing new block at height 519895
Processing new block at height 519896
Writing 160 transactions
Processing new block at height 519897
Processing new block at height 519898
Processing new block at height 519899
Writing 167 transactions
Writing 187 transactions
Writing 165 transactions
Writing 175 transactions
Launching batch job at 2018-04-25T192329Z
Launching Beam job at 2018-04-25T192329Z
Launching InfluxDB insert at 2018-04-25T192329Z
Writing 150 transactions
```

The pipeline will start collecting data and running. Results will appear in the Grafana dashboard in ~10 minutes, so go ahead and open up and start configuring Grafana.


## Using Grafana

![](grafana_dashboard.png)

Access the Grafana dashboard at `localhost:3000`. The default credentials are `admin` and `admin`.
Add a data source named `unconfirmed` using `InfluxDB` as the type. Use URL `http://localhost:8086` with `proxy` access. The database name is `blockchain`, and the Min time intervals should be `5m`.

Create your own visualizations for the plotting unconfirmed transaction volume across time. Alternatively, import `grafana_dashboard.json` to get started. The Grafana documentation claims that the `.json` file _should_ contain all of the configured data sources for a dashboard--the the manual configuration described above should be unnecessary--but in testing this was not found to be the case.


## Troubleshooting

Even though an API key is used to attempt to avoid API rate-limiting, timeout errors can occur. If necessary, tweak `SLEEP_TIME` in `fetch_mempool.py`. The default value of 2 should provide a reasonable balance. Sleep times as low as 1 were tested and proved stable in some circumstances but failed in others. Running this with a high sleep values (in particular less than once every 10 seconds) will result in missed transactions, however, so we desire to keep this sleep time as low as possible. If there are failures, simply exit the Docker container, rebuild with a longer `SLEEP_TIME`, and launch the container again.

Both Grafana and InfluxDB have their ports mapped outside the container, so you can launch `influx` on your host OS and access data inside InfluxDB to see the data that is stored.
```
$ influx
Connected to http://localhost:8086 version 1.5.2
InfluxDB shell version: v1.5.2
> use blockchain;
Using database blockchain
> show measurements;
name: measurements
name
----
unconfirmed_transactions
> select * from unconfirmed_transactions;
name: unconfirmed_transactions
time                count
----                -----
1524684209000000000 1017
```

# Conclusions and Lesson Learned
* Like Spark, Apache Beam uses lazy evaluation and does not perform calculations on results until the final result must be computed. Unfortunately, beam does not have a REPL for examining intermediate data. This means debugging required writing out complete jobs that write to output files and slowly and incrementally adding on functionality. However, PySpark makes examining intermediate results for debugging very easy, so although the Apache Beam model seeks to unite the streaming/batch APIs, it seems somewhat more difficult to develop for compared to Spark's established toolset.
* Two major limitations exist in this pipeline and technologies: the query frequency of the public blockchain.info API and Apache Beam's IO format support. The blockchain.info API was on some occassions stable when queried once per second, but on other occassions timeout errors occured. Beam's limited Python support meant that reading and writing from filesystem was necessary instead of directly writing to a database or messaging service as would be ideal.
* If implementing this project a second time, more care would be taken on the collection tier for ways to make collection robust. In particular, the current collection tier code performs more than simple collection: right now it also performs some deduplication to avoid writing redundant data that Apache Beam will remove anyway. Although the data volume for a single thread here is reasonable (and in fact bounded by the Bitcoin network to be approximately 400KB/minute), we ideally would record all data we received and then remove duplicates in a single location later. If we were collecting data from our own `bitcoind` node instead of from Blockchain.info, this problem could also go away, and our collection tier would be a very simple `tail -f` piped to `grep` on the node logs.
* Since the Blockchain.info API was the main source of job failure and running your own network node takes several weeks to sync with the chain, the natural improvement with more time would be to start your own node for collection instead. Once the initial network synchronization completes, the collection tier would become both more simple and robust.
* The batch job that runs in Apache Beam would be a good location for alternative technologies. If we desire to operate this with streaming support, we could use Spark, Flink, or Storm here. These also have better IO support with connectors to additional databases, so these would have been good choices in the pipeline.
* The obvious enhancement to this project would be to utilize your own `bitcoind` node instead of relying on the blockchain.info APIs. By tailing the logs from our own node, we could guarantee that we do not miss events. Furthermore, we could set our own mempool parameters and run several nodes with very permissive retention policies so as to collect transactions that blockchain.info may never have received. Additionally, if we wish to run this project for many months instead of merely for a few hours, we should lower the batch frequency to an acceptable level (e.g. once per day) since the amount of data will continue to grow as more transactions occur.
