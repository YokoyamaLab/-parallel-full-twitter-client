const { io } = require("socket.io-client");
const socket = io("ws://tokyo:3000/");

socket.on('connect',function(){
    console.log('yea!!');
    socket.on("launch-ok", (msg) => {
        console.log(msg);
    });
    socket.emit('launch',{"origin":"test"});
    //socket.disconnect();
    //process.exit(0);
});
