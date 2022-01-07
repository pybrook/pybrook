<script>
    import LeafletMap from './LeafletMap.svelte';
    import {tweened} from 'svelte/motion';
    import {cubicOut} from 'svelte/easing';
    import Marker from './Marker.svelte';
    import 'carbon-components-svelte/css/all.css';
    import reportStore from './reportStore';

    import {
        Accordion,
        AccordionItem,
        ClickableTile,
        Column,
        Grid, InlineNotification, Link,
        Modal,
        Row
    } from "carbon-components-svelte";

    let vehicles = {}
    let groups = {};
    let vehiclePositions = {};
    let vehicleReports = {};
    let openedGroups = new Set();
    let openGroup = {};


    reportStore.subscribe(
        (value) => {
            if(!value) return;
            let vehicleId = value["vehicle_id"];
            if(!vehicles.hasOwnProperty(vehicleId)){
                let group = value["line"];
                if(!groups[group]){
                    groups[group] = new Set();
                }
                groups[group] = groups[group].add(vehicleId);
                vehicles[vehicleId] = group
                vehicleReports[vehicleId] = value;
            }

            vehiclePositions[vehicleId] = {
                lat: value["latitude"],
                lon: value["longitude"]
            }

            if(vehicleId == modalVehicleId){
                console.log(value);
                modalVehicleData = Object.entries(value);
            }
        }
    )
    let vehiclesInViewPort = [];
    $: vehicleEntries = []

    let theme = "white"; // "white" | "g10" | "g80" | "g90" | "g100"

    $: document.documentElement.setAttribute("theme", theme);


    let modalOpen = false;
    let modalVehicleId = undefined;
    let modalVehicleData = [];
</script>
<Grid fullWidth>

    <Row>
        <Column padding xs={4} sm={4} md={8} lg={8} xlg={12} aspectRatio="1x1">
            <LeafletMap let:map={map} on:moveend={({detail}) => {
                let newVehiclesInViewPort = []
                let {bounds} = detail;
                Object.entries(vehiclePositions).forEach(
                    ([vehicleId, {lat, lon}]) => {
                        if (bounds.contains([lat, lon]))
                            newVehiclesInViewPort.push(vehicleId)
                    }
                )
                vehiclesInViewPort = newVehiclesInViewPort;
                console.log(vehiclesInViewPort)
            }}>
                {#if vehiclesInViewPort.length <= 400}
                {#each vehiclesInViewPort as vehicleId}
                {#key vehicleId}
                    <Marker {map} lat={vehiclePositions[vehicleId].lat} lng={vehiclePositions[vehicleId].lon} on:click={() => {modalOpen = true; modalVehicleId = vehicleId; modalVehicleData = Object.entries(vehicleReports[vehicleId])}}/>
                {/key}
                {/each}
                {/if}

            </LeafletMap>


            </Column>
        <Column padding xs={4} sm={4} md={8} lg={8} xlg={4}>
            {#if vehiclesInViewPort.length > 400}
                <InlineNotification kind="warning" title="Too many vehicles in viewport: " subtitle="please zoom in to show vehicle positions" hideCloseButton/>
            {/if}
            <div style="overflow-y: auto;max-height: 80vh;">
                <Accordion>
                    {#each Object.entries(groups) as [group, vehicles]}
                        {#key group}
                        <AccordionItem title="{group}" bind:open={openGroup[group]}>
                                    {#if openGroup[group]}
                                    {#each Array.from(vehicles) as vehicleId}
                                        {@debug vehicleId}
                                        <p><Link on:click={() => {modalOpen = true; modalVehicleId = vehicleId; modalVehicleData = Object.entries(vehicleReports[vehicleId])}}>{vehicleId}</Link></p>
                                    {/each}
                                    {/if}
                        </AccordionItem>
                        {/key}
                    {/each}
                </Accordion>
            </div>
        </Column>
    </Row>
</Grid>

<Modal
        size="lg"
        open="{modalOpen}"
        modalHeading="Vehicle {modalVehicleId}"
        primaryButtonText="Close"
        on:close={() => modalOpen = false}
        on:submit={() => modalOpen = false}
>
    <Accordion>
        {#each modalVehicleData as [field, value]}
        <AccordionItem title="{field}" open>
            <p>
                {value}
            </p>
        </AccordionItem>
        {/each}
    </Accordion>
</Modal>
<style>
    .bordered-tile {
        border-bottom: 1px solid #aaa;
    }
    #move-btn {
        position: absolute;
        z-index: 1000;
        bottom: 50px;
        left: 20px;
    }
</style>
