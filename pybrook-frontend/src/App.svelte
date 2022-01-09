<script>
    import LeafletMap from './LeafletMap.svelte';
    import VehicleMarker from './VehicleMarker.svelte';
    import 'carbon-components-svelte/css/all.css';
    import {reportStore, genericReportStore} from './stores.js';

    import {
        Accordion,
        AccordionItem,
        Column,
        Grid, InlineNotification,
        Modal,
        Row
    } from "carbon-components-svelte";
    import Light24 from "carbon-icons-svelte/lib/Light24"

    import Moon24 from "carbon-icons-svelte/lib/Moon24"
    import VehicleList from "./VehicleList.svelte";
    import VehiclesMap from "./VehiclesMap.svelte";
    import VehicleDetailModal from "./VehicleDetailModal.svelte";

    let vehicles = {}
    let groups = {};
    let notificationHeight;
    const darkModeQuery = window.matchMedia('(prefers-color-scheme: dark)');
    let theme = darkModeQuery.matches ? "g100" : "white"; // "white" | "g10" | "g80" | "g90" | "g100"
    $: document.documentElement.setAttribute("theme", theme);
    let modalOpen = false;
    let modalInTransition = false;
    let modalVehicleId = undefined;
    let tooManyVehicles = false;
</script>
<nav>
    <h4 class="navbar-brand">PyBrook</h4>
    {#if theme != "white"}
        <Light24 style="cursor:pointer;" on:click={() => theme = "white"}/>
    {:else}
        <Moon24 style="cursor:pointer;" on:click={() => theme = "g100"}/>
    {/if}
</nav>
<Grid fullWidth>

    <Row>
        <Column padding xs={4} sm={4} md={8} lg={8} xlg={12}>
            <VehiclesMap let:tooManyVehicles theme={theme} on:vehicle-selected={({detail}) => {modalOpen = true; modalVehicleId = detail.vehicleId;}}/>
        </Column>
        <Column padding xs={4} sm={4} md={8} lg={8} xlg={4}>
            <div id="vehicle-list-wrapper" style="overflow-y: auto;max-height: calc(100vh - 40px - 3rem);">
                <VehicleList on:vehicle-selected={({detail}) => {modalOpen = true; modalVehicleId = detail.vehicleId;}}/>
            </div>
        </Column>
    </Row>
</Grid>
{#if modalOpen || modalInTransition}
    <VehicleDetailModal open={modalOpen} on:transitionend={() => modalInTransition = false} on:close={() => {modalOpen = false; modalInTransition = true; }} vehicleId={modalVehicleId}/>
{/if}
<style>
    .navbar-brand {
        font-weight: bold;
        flex-grow: 1;
    }
    @media screen and (max-width: 1056px){
        #vehicle-list-wrapper {
            max-height: none !important;
            overflow-y: hidden !important;
        }
    }
    nav {
        padding-left: 2.2rem;
        padding-right: 2.2rem;
        display: flex;
        align-items: center;
        height: 3rem;
        border-bottom: 1px transparent var(--cds-text-01);
        box-shadow: 0 1px 2px 0 var(--cds-text-01);
    }

</style>
