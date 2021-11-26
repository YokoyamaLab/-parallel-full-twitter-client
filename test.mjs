import { io } from "socket.io-client";
import * as fs from 'fs';
import async from "async";
import path from 'path';
import lz4 from 'lz4';
import readline from 'readline';
import { DateTime, Interval } from 'luxon';
import jsonMask from 'json-mask';
import { Piscina } from 'piscina';

import fastjson from 'fastjson';
import { MessageChannel } from 'worker_threads';

const files = ["2021-11-02-00-00-00.01.lz4", "2021-11-02-04-00-00.01.lz4",
    "2021-11-02-00-10-00.01.lz4", "2021-11-02-04-10-00.01.lz4",
    "2021-11-02-00-20-00.01.lz4", "2021-11-02-04-20-00.01.lz4",
    "2021-11-02-00-30-00.01.lz4", "2021-11-02-04-30-00.01.lz4",
    "2021-11-02-00-40-00.01.lz4", "2021-11-02-04-40-00.01.lz4",
    "2021-11-02-00-50-00.01.lz4", "2021-11-02-04-50-00.01.lz4",
    "2021-11-02-01-00-00.01.lz4", "2021-11-02-05-00-00.01.lz4",
    "2021-11-02-01-10-00.01.lz4", "2021-11-02-05-10-00.01.lz4",
    "2021-11-02-01-20-00.01.lz4", "2021-11-02-05-20-00.01.lz4",
    "2021-11-02-01-30-00.01.lz4", "2021-11-02-05-30-00.01.lz4",
    "2021-11-02-01-40-00.01.lz4", "2021-11-02-05-40-00.01.lz4",
    "2021-11-02-01-50-00.01.lz4", "2021-11-02-05-50-00.01.lz4",
    "2021-11-02-02-00-00.01.lz4", "2021-11-02-06-00-00.01.lz4",
    "2021-11-02-02-10-00.01.lz4", "2021-11-02-06-10-00.01.lz4",
    "2021-11-02-02-20-00.01.lz4", "2021-11-02-06-20-00.01.lz4",
    "2021-11-02-02-30-00.01.lz4", "2021-11-02-06-30-00.01.lz4",
    "2021-11-02-02-40-00.01.lz4", "2021-11-02-06-40-00.01.lz4",
    "2021-11-02-02-50-00.01.lz4", "2021-11-02-06-50-00.01.lz4",
    "2021-11-02-03-00-00.01.lz4", "2021-11-02-07-00-00.01.lz4",
    "2021-11-02-03-10-00.01.lz4", "2021-11-02-07-10-00.01.lz4",
    "2021-11-02-03-20-00.01.lz4", "2021-11-02-07-20-00.01.lz4",
    "2021-11-02-03-30-00.01.lz4", "2021-11-02-07-30-00.01.lz4",
    "2021-11-02-03-40-00.01.lz4", "2021-11-02-07-40-00.01.lz4",
    "2021-11-02-03-50-00.01.lz4", "2021-11-02-07-50-00.01.lz4"
];

const piscina = new Piscina({
    filename: new URL('./workers/search.mjs', import.meta.url).href,
    //maxThreads: 30
});
const startTime = DateTime.now();
let results = await Promise.all(files.map((file) => {
    return new Promise((resolve, reject) => {
        file = "/data/local/twitter.8/2021-11-02/" + file;
        const query = {
            mask: "id_str,text,user(id_str,name,screen_name,followers_count,friends_count),geo,in_reply_to_user_id_str,in_reply_to_status_id_str,timestamp_ms,created_at",
            keywords: ["コロナ"]
        };
        const channel = new MessageChannel();
        channel.port2.on('message', (message) => {
            if (message.type == "message") {
                console.log(file, message.message);
            } else if (message.type == "hit") {
                //console.log(message.tweet.text);
            } else if (message.type == "finish") {
                console.log(file);
                console.log("Hits:", message.nHits + " / " + message.nTweets);
                channel.port2.close();
                resolve({
                    nHits: message.nHits,
                    nTweets: message.nTweets,
                    execTime: message
                });
            }
        });
        piscina.runTask({
            port: channel.port1,
            query: query,
            file: file
        }, [channel.port1]);
    });
}));
const finishTime = DateTime.now();
const execTime = finishTime.diff(startTime, 'seconds');
//console.log(results);
const stats = results.reduce((memo, result) => {
    memo.nHits += result.nHits;
    memo.nTweets += result.nTweets;
    return memo;
}, { nHits: 0, nTweets: 0 });
console.log(stats);
console.log(execTime.seconds, "seconds");
