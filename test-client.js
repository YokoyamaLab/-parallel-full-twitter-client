const { io } = require("socket.io-client");
const socket = io("ws://tokyo:3000/");
const HOST_NAME = process.env.HOSTNAME;

socket.on('connect',function(){
    console.log('yea!!');
    socket.on("launch-ok", (msg) => {
        console.log(msg);
    });
    socket.emit('launch',{"origin":HOST_NAME});
    //socket.disconnect();
    //process.exit(0);
});
