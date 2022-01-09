<script>
    import {configStore, latestReports, reportStore} from "./stores";
    import {
        Accordion,
        AccordionItem,
        Modal, SkeletonText,
    } from "carbon-components-svelte";
    import {createEventDispatcher, onDestroy} from "svelte";
    const dispatch = createEventDispatcher();
    export let open;
    export let vehicleId;
    let modalData = {};
    function setLatest(vehicleId){
        if(!vehicleId) return;
        if ($latestReports.hasOwnProperty(vehicleId)){
            modalData = $latestReports[vehicleId];
        } else {
            modalData = {}
        }
    }

    const unsubscribe = reportStore.subscribe((data) => {
        if(!data) return;
        if(vehicleId == data.vehicleId)
        modalData[data.streamName] = data.report;
    })
    let latestTime;
    let latestMessageId;
    let now = new Date();

    $: {
        if($configStore && modalData.hasOwnProperty($configStore.time_field.stream_name)){
            let latestData = modalData[$configStore.time_field.stream_name];
            latestTime = latestData.originalMessageTime;
            latestMessageId = latestData.messageId;
        }
    }
    const interval = setInterval(() => now = new Date(), 100);
    onDestroy(() => {unsubscribe(); clearInterval(interval)});
    $: setLatest(vehicleId);
    function onClose(){
        dispatch('close');
    }
</script>
<Modal
        size="lg"
        open="{open}"
        modalHeading="Vehicle {vehicleId}"
        primaryButtonText="Close"
        on:close={onClose}
        on:submit={onClose}
>
    {#if $configStore && vehicleId && open}
        <Accordion>
            {#each $configStore.streams as {stream_name, report_schema: {properties}} (stream_name)}
                <AccordionItem open>
                    <div slot="title">
                        {stream_name}

                        {#if modalData.hasOwnProperty(stream_name)}
                            {#if $configStore.time_field.stream_name === stream_name}
                                <div style="color:green">
                                    Latest report, {(now - new Date(modalData[stream_name].originalMessageTime)) / 1000} seconds old
                                </div>
                            {:else}
                                {#if modalData[stream_name].messageId === latestMessageId}
                                    <div style="color:green">
                                        Based on latest report, {(now - new Date(modalData[stream_name].originalMessageTime)) / 1000} seconds old
                                    </div>
                                {:else}
                                    <div style="color:orange">
                                        {(new Date() - new Date(modalData[stream_name].originalMessageTime)) / 1000} seconds behind the latest report
                                    </div>
                                {/if}
                            {/if}
                        {/if}
                    </div>
                        {#each Object.entries(properties) as [key, {title}] (key)}
                            <h5>{title}</h5>
                            {#if modalData.hasOwnProperty(stream_name)}
                                {modalData[stream_name].report[key]}
                            {:else}
                                <SkeletonText/>
                            {/if}
                        {/each}
                </AccordionItem>
            {/each}
        </Accordion>
    {/if}
</Modal>
