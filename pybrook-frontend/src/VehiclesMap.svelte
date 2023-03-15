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
    let vehicleDirections = {};
    let vehicleIdToGroup = {};
    let mapNotificationHeight;
    let selected;
    const dispatch = createEventDispatcher();

    $: {
        if ($configStore) {
            vehicleIdToGroup = {};
            vehiclePositions = {};
            selected = null;
            vehiclesInViewPort = [];
            vehicleDirections = {};
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
        if(data.direction){
            vehicleDirections[vehicleId] = data.direction;
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
            <VehicleMarker {map}
                           directionDeg={vehicleDirections[vehicleId]}
                           lat={vehiclePositions[vehicleId].lat}
                           lng={vehiclePositions[vehicleId].lon}
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

