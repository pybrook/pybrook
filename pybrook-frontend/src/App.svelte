<script>
    import LeafletMap from './LeafletMap.svelte';
    import Marker from './Marker.svelte';
    import 'carbon-components-svelte/css/all.css';
    import reportStore from './reportStore';

    import {
        Accordion,
        AccordionItem,
        UnorderedList,
        ListItem,
        Column,
        Grid, InlineNotification, Link,
        Modal,
        Row
    } from "carbon-components-svelte";

    let vehicles = {}
    let groups = {};
    let groupKeys = new Set();
    let vehiclePositions = {};
    let vehicleReports = {};
    let openGroups = {};
    let notificationHeight;

    reportStore.subscribe(
        (value) => {
            if (!value) return;
            let vehicleId = value["vehicle_id"];
            if (!vehicles.hasOwnProperty(vehicleId)) {
                let group = value["line"];
                if (!groups[group]) {
                    groups[group] = new Set();
                    groupKeys = groupKeys.add(group);
                }
                groups[group] = groups[group].add(vehicleId);
                vehicles[vehicleId] = group
                vehicleReports[vehicleId] = value;
            }

            vehiclePositions[vehicleId] = {
                lat: value["latitude"],
                lon: value["longitude"]
            }

            if (vehicleId == modalVehicleId) {
                console.log(value);
                modalVehicleData = Object.entries(value);
            }
        }
    )
    let vehiclesInViewPort = [];

    let theme = "white"; // "white" | "g10" | "g80" | "g90" | "g100"

    $: document.documentElement.setAttribute("theme", theme);


    let modalOpen = false;
    let modalVehicleId = undefined;
    let modalVehicleData = [];

    $: tooManyVehicles = vehiclesInViewPort.length > 100
</script>
<Grid fullWidth>

    <Row>
        <Column padding xs={4} sm={4} md={8} lg={8} xlg={12}>
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
                {#if !tooManyVehicles}
                    {#each vehiclesInViewPort as vehicleId}
                        {#key vehicleId}
                            <Marker {map} lat={vehiclePositions[vehicleId].lat} lng={vehiclePositions[vehicleId].lon}
                                    on:click={() => {modalOpen = true; modalVehicleId = vehicleId; modalVehicleData = Object.entries(vehicleReports[vehicleId])}}/>
                        {/key}
                    {/each}
                {/if}

            </LeafletMap>


        </Column>
        <Column padding xs={4} sm={4} md={8} lg={8} xlg={4}>
            {#if tooManyVehicles}
                <div bind:clientHeight={notificationHeight}>
                    <InlineNotification kind="warning" title="Too many vehicles in viewport: "
                                        subtitle="Please zoom in to show vehicle positions" hideCloseButton/>
                </div>
            {/if}
            <div style={tooManyVehicles ? `overflow-y: auto;max-height: calc(100vh - ${notificationHeight}px - 80px);`: "overflow-y: auto;max-height: calc(100vh - 40px);" }>
                <Accordion>
                    {#each Array.from(groupKeys).sort() as group (group)}
                        <AccordionItem title="{group}" bind:open={openGroups[group]}>
                            {#if openGroups[group]}
                            <UnorderedList>
                                {#each Array.from(groups[group]) as vehicleId (vehicleId)}
                                    <ListItem>
                                        <Link style="cursor: pointer;" on:click={() => {modalOpen = true; modalVehicleId = vehicleId; modalVehicleData = Object.entries(vehicleReports[vehicleId])}}>{vehicleId}</Link>
                                    </ListItem>
                                {/each}
                            </UnorderedList>
                            {/if}
                        </AccordionItem>
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
        {#each modalVehicleData as [field, value] (field)}
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
