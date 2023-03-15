/*
 * PyBrook
 *
 * Copyright (C) 2023  Michał Rokita
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 *
 */

import { writable } from 'svelte/store';

export const reportStore = writable(null);
export const genericReportStore = writable(null);
export const configStore = writable(null);
export const latestReports = writable({});

const apiUrl = window.location.host;
const apiWsUrl = `ws://${apiUrl}`;
const apiHttpUrl = `http://${apiUrl}`;
let webSockets = []
export let messageTimes = {};

configStore.subscribe((config) => {
    if(!config) return;
    console.log("Received new config!", config)
    webSockets.forEach((s) => s.close())
    let closed=false;
    let {latitude_field, time_field, direction_field, longitude_field, group_field, special_char, msg_id_field} = config;
    config.streams.forEach(({stream_name, websocket_path}) => {
        let socket = new WebSocket(`${apiWsUrl}${websocket_path}`)
        let useLatitude = latitude_field.stream_name === stream_name;
        let useLongitude = longitude_field.stream_name === stream_name;
        let useDirection = direction_field && direction_field.stream_name === stream_name;
        let useGroup = group_field.stream_name === stream_name;
        let useTime = time_field.stream_name === stream_name
        let containsGeneric = useLongitude || useLatitude || useGroup || useDirection;
        socket.addEventListener('message', ({data}) => {
            if(!data) return;
            data = JSON.parse(data);
            let messageId = data[msg_id_field];
            let [vehicleMessageId, ...vehicleIdParts] = messageId.split(special_char).reverse()
            let vehicleId = vehicleIdParts.reverse().join(special_char)
            let originalMessageTime = null;
            if(useTime) {
                originalMessageTime = data[time_field.field_name];
                messageTimes[messageId] = originalMessageTime;
            }
            let reportStoreData = {streamName: stream_name, originalMessageTime: messageTimes[messageId], messageId, vehicleMessageId, vehicleId, report: data};
            reportStore.set(reportStoreData);
            latestReports.update(
                (data) => {
                    if(!data.hasOwnProperty(vehicleId)){
                        data[vehicleId] = {};
                    }
                    data[vehicleId][stream_name] = reportStoreData;
                    return data;
                }
            )
            if(!containsGeneric) return;
            let genericData = {}
            if(useLatitude) genericData.latitude =  data[latitude_field.field_name]
            if(useLongitude) genericData.longitude =  data[longitude_field.field_name]
            if(useGroup) genericData.group = data[group_field.field_name]
            if(useTime) genericData.time = originalMessageTime;
            if(useDirection) genericData.direction = data[direction_field.field_name]
            genericReportStore.set({vehicleId, vehicleMessageId, messageId, data: genericData});
        })
        socket.addEventListener('close', (data) => {
            if(closed) return;
            closed = true;
            setTimeout(loadConfig, 1000);
        });
    })

});


function loadConfig() {
    fetch(`${apiHttpUrl}/pybrook-schema.json`).then(res => res.json()).then(
        (json) => {
            if(json.streams && json.group_field) {
                configStore.set(json);
            } else {
                setTimeout(loadConfig, 1000)
            }
        }
    ).catch((err) => setTimeout(loadConfig, 1000))
}

loadConfig();
