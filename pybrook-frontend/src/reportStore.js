import { writable } from 'svelte/store';

const reportStore = writable(null);

const socket = new WebSocket('ws://localhost:8000/location-report');

// Connection opened
socket.addEventListener('open', function (event) {
    console.log("It's open");
});

// Listen for messages
socket.addEventListener('message', function (event) {
    reportStore.set(JSON.parse(event.data))
});

export default reportStore
