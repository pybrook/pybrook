<script>
    import {ClickableTile} from "carbon-components-svelte";
    import reportStore from "./reportStore";
    let vehicles = {};
    reportStore.subscribe(
        (value) => {
            if(!vehicles.hasOwnProperty(value["vehicle_id"]))
                vehicles[value["vehicle_id"]] = value
        }
    )
    $: vehicleEntries = Object.entries(vehicles);
</script>
{#each vehicleEntries as [vehicleId, report]}
    <div class="bordered-tile">
        <ClickableTile>{vehicleId}<br/><span style="color:#333">Linia {report["line"]}</span></ClickableTile>
    </div>
{/each}
