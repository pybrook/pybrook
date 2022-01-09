<script>

    import LeafletMap from "./LeafletMap.svelte";
    import VehicleMarker from "./VehicleMarker.svelte";
    import {configStore, genericReportStore} from "./stores";
    import {createEventDispatcher, onDestroy} from "svelte";
    import {InlineNotification} from "carbon-components-svelte";

    export let theme;
    let tooManyVehicles = false;
    let vehiclesInViewPort = [];
    let bounds;
    let vehiclePositions = {};
    let vehicleIdToGroup = {};
    let mapNotificationHeight;
    let selected;
    const dispatch = createEventDispatcher();

    $: {
        if ($configStore) {
            vehicleIdToGroup = {};
            vehiclePositions = new Set();
            selected = null;
            vehiclesInViewPort = [];
        }
    }
    const unsubscribe = genericReportStore.subscribe((msg) => {
        if (!msg) return;
        let {vehicleId, data} = msg;
        if(data.group) {
            vehicleIdToGroup[vehicleId] = data.group;
        }
        if(data.latitude && data.longitude){
            vehiclePositions[vehicleId] = {lat: data.latitude, lon: data.longitude};
        }
    });
    onDestroy(unsubscribe);

    $: {
        if (bounds) {
            let newVehiclesInViewPort = []
            Object.entries(vehiclePositions).forEach(
                ([vehicleId, {lat, lon}]) => {
                    if (bounds.contains([lat, lon]))
                        newVehiclesInViewPort.push(vehicleId)
                }
            )
            vehiclesInViewPort = newVehiclesInViewPort;
            tooManyVehicles = vehiclesInViewPort.length > 120;
        }
    }

</script>

<LeafletMap let:map={map} theme={theme} on:moveend={({detail}) => {
                bounds = detail.bounds;
            }}>
        <div class="map-notification" slot="notification" bind:clientHeight={mapNotificationHeight}>
            {#if tooManyVehicles }
            <InlineNotification kind="warning" title="Too many vehicles in viewport: "
                                subtitle="Please zoom in to show vehicle positions" hideCloseButton/>
            {/if}
        </div>
    {#if !tooManyVehicles}
        {#each vehiclesInViewPort as vehicleId (vehicleId)}
            <VehicleMarker {map} lat={vehiclePositions[vehicleId].lat} lng={vehiclePositions[vehicleId].lon}
                           selected={selected == vehicleId}
                           labelText="{vehicleIdToGroup[vehicleId]}/{vehicleId}"
                           on:click={() => {dispatch('vehicle-selected', {vehicleId}); selected=vehicleId;}}/>
        {/each}
    {/if}
</LeafletMap>

<style>
    .map-notification {
        z-index: 1000;
        position: absolute;
        bottom: 0px;

    }
</style>

