<!--
  - PyBrook
  -
  - Copyright (C) 2023  Michał Rokita
  -
  - This program is free software: you can redistribute it and/or modify
  - it under the terms of the GNU General Public License as published by
  - the Free Software Foundation, either version 3 of the License, or
  - (at your option) any later version.
  -
  - This program is distributed in the hope that it will be useful,
  - but WITHOUT ANY WARRANTY; without even the implied warranty of
  - MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  - GNU General Public License for more details.
  -
  - You should have received a copy of the GNU General Public License
  - along with this program.  If not, see <https://www.gnu.org/licenses/>.
  -
  -->

<script>
    import {onDestroy} from "svelte";
    import {configStore, messageTimes} from "./stores";

    export let streamData, streamName, latestMessageId, latestTime, latestVehicleMessageId

    let now = new Date();

    const interval = setInterval(() => now = new Date(), 100);
    onDestroy(() =>  clearInterval(interval))
</script>
{#if $configStore.time_field.stream_name === streamName}
    <div style="color:green">
        Latest report, {(now - new Date(streamData.originalMessageTime)) / 1000} seconds old
    </div>
{:else}
    {#if streamData.messageId === latestMessageId}
        <div style="color:green">
            Based on latest report (id: {latestMessageId}), which is {(now - new Date(latestTime)) / 1000} seconds old
        </div>
    {:else}
        <div style="color:orange">
            {latestVehicleMessageId - streamData.vehicleMessageId} records behind{#if messageTimes[streamData.messageId]}, {(now - new Date(messageTimes[streamData.messageId])) / 1000} seconds old{/if}
        </div>
    {/if}
{/if}