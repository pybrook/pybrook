<script>
    import {configStore, latestReports, reportStore} from "./stores";
    import {
        Accordion,
        AccordionItem,
        Modal, SkeletonText,
    } from "carbon-components-svelte";
    import {createEventDispatcher, onDestroy} from "svelte";
    import ReportTimeElapsed from "./ReportTimeElapsed.svelte";

    const dispatch = createEventDispatcher();
    export let open;
    export let vehicleId;
    let openStreamAccordions = {};
    let modalData = {};

    function setLatest(vehicleId) {
        if (!vehicleId) return;
        if ($latestReports.hasOwnProperty(vehicleId)) {
            modalData = $latestReports[vehicleId];
            let latestData = modalData[$configStore.time_field.stream_name];
            latestTime = latestData.originalMessageTime;
            latestMessageId = latestData.messageId;
            latestVehicleMessageId = latestData.vehicleMessageId
        } else {
            modalData = {}
        }
    }

    const unsubscribe = reportStore.subscribe((data) => {
        if (!data) return;
        if (vehicleId == data.vehicleId)
            modalData[data.streamName] = data.report;
    })
    let latestTime;
    let latestMessageId;
    let latestVehicleMessageId;

    onDestroy(unsubscribe);
    $: $configStore && setLatest(vehicleId) && modalData && open;

    function onClose() {
        dispatch('close');
    }

    function clearData() {
        latestMessageId = null;
        latestTime = null;
        latestVehicleMessageId = null;
        modalData = {};
    }
</script>
<Modal
        size="lg"
        open="{open}"
        modalHeading="Vehicle {vehicleId}"
        primaryButtonText="Close"
        on:transitionend={clearData}
        on:close={onClose}
        on:submit={onClose}
>
    {#if $configStore && vehicleId}
        <Accordion>
            {#each $configStore.streams as {stream_name, report_schema: {properties}} (stream_name)}
                <AccordionItem>
                    <div slot="title">
                        {stream_name}

                        {#if modalData.hasOwnProperty(stream_name)}
                            <ReportTimeElapsed streamData={modalData[stream_name]}
                                               streamName={stream_name}
                                               latestMessageId={latestMessageId}
                                               latestTime={latestTime}
                                               latestVehicleMessageId={latestVehicleMessageId}/>
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
