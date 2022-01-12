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
            {latestVehicleMessageId - streamData.vehicleMessageId} records behind, {(now - new Date(messageTimes[streamData.messageId])) / 1000} seconds old
        </div>
    {/if}
{/if}