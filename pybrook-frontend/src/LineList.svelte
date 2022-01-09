<script>
    import {ClickableTile} from "carbon-components-svelte";
    import reportStore from "./stores";
    let vehicles = {};
    reportStore.subscribe(
        (value) => {
            if(!vehicles.hasOwnProperty(value["vehicle_id"]))
                vehicles[value["vehicle_id"]] = value
        }
    )
    let vehicleEntries;
    $: vehicleEntries = Object.entries(vehicles);
</script>
{#each vehicleEntries as [vehicleId, report]}
    <div class="bordered-tile">
        <ClickableTile>{vehicleId}<br/><span style="color:#333">Linia {report["line"]}</span></ClickableTile>
    </div>
{/each}
