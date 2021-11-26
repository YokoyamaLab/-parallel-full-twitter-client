import { io } from "socket.io-client";
import * as fs from 'fs';
//import async from "async";
//import path from 'path';
//import lz4 from 'lz4';
//import readline from 'readline';
//import { DateTime, Interval } from 'luxon';
//import jsonMask from 'json-mask';
import { Piscina } from 'piscina';
import { MessageChannel } from 'worker_threads';

const HOST_NAME = (await fs.promises.readFile("/etc/hostname")).toString().trim();
console.log(HOST_NAME);
const BASE_DIR = process.env.PTWEET_BASE;
const PORT = process.env.PTWEET_PORT;
const SERVER = process.env.PTWEET_SERVER;
const sertURL = "ws://" + SERVER + ":" + PORT + "/";

console.log("URL:", sertURL);
const socket = io(sertURL);

const piscina = new Piscina({
    filename: new URL('./workers/search.mjs', import.meta.url).href,
    maxThreads: 30
});

socket.on("connect_error", () => {
    console.log("connect_error");
});
socket.on("disconnect", (reason) => {
    console.log("[disconnect]",reason);
});

socket.on('connect', function () {
    /*socket.on("disconnect", () => {
        console.log("disconnected");
        //process.exit();
    });*/
    socket.once("launch-return", (msg) => {
        //console.log(msg);
    });
    socket.on("query", (msg) => {
        //console.log("query:", msg.file);
        const channel = new MessageChannel();
        channel.port2.on('message', (rtn) => {
            if (rtn.type == "message") {
                //socket.emit("message", message);
            } else if (rtn.type == "finish") {
                //channel.port2.close();
                socket.emit("query-finish", rtn);
            }
        });
        piscina.runTask({
            port: channel.port1,
            query: msg.query,
            file: msg.file,
            fileN: msg.fileN,
            startTime: msg.startTime,
            client: msg.client
        }, [channel.port1]);
    });
    socket.once("kill", (msg) => {
        console.log("[KILL by server]");
        process.exit();
    });
    socket.emit('launch', { "origin": HOST_NAME + "-10g" });
});
