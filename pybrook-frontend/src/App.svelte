<script>
    import markerIconPng from 'leaflet/dist/images/marker-icon.png'
    import LeafletMap from './LeafletMap.svelte';
    import {tweened} from 'svelte/motion';
    import {cubicOut} from 'svelte/easing';
    import Marker from './Marker.svelte'
    import 'carbon-components-svelte/css/all.css'
    import {
        Accordion,
        AccordionItem,
        ClickableTile,
        Column,
        Grid,
        Modal,
        Row,
        SelectableTile
    } from "carbon-components-svelte";

    let posAnim = (initial) => tweened(initial, {
        duration: 1000,
        easing: cubicOut
    });
    const lng = posAnim(-0.07);
    const lat = posAnim(51.5);
    let theme = "white"; // "white" | "g10" | "g80" | "g90" | "g100"

    $: document.documentElement.setAttribute("theme", theme);

    function moveMarker() {
        lng.set($lng + 0.001);
        lat.set($lat + 0.001);
    }

    let modalOpen = false;
</script>
<Grid fullWidth>

    <Row style="height: 100vh;padding-top:20px;padding-bottom:20px;">
        <Column xs={4} sm={4} md={8} lg={8} xlg={12}>
            <LeafletMap let:map>
                <Marker {map} lat={$lat} lng={$lng} on:click={() => modalOpen = true}/>
                <Marker {map} lat={51.5} lng={-0.08} on:click={() => modalOpen = true}/>
            </LeafletMap>
        </Column>
        <Column xs={4} sm={4} md={8} lg={8} xlg={4} style="overflow-y: auto;max-height: 100%;">
            {#each Array(10000) as unused}
            <div class="bordered-tile">
                <ClickableTile class="xd" on:click={() => modalOpen = true}>523<br/><span style="color:#333">Linia 523</span></ClickableTile>
            </div>
            {/each}
        </Column>
    </Row>
</Grid>

<Modal
        size="lg"
        open="{modalOpen}"
        modalHeading="Vehicle 523"
        primaryButtonText="Close"
        on:close={() => modalOpen = false}
        on:submit={() => modalOpen = false}
>
    <Accordion>
        <AccordionItem title="Natural Language Classifier">
            <p>
                Natural Language Classifier uses advanced natural language processing and
                machine learning techniques to create custom classification models. Users
                train their data and the service predicts the appropriate category for the
                inputted text.
            </p>
        </AccordionItem>
        <AccordionItem title="Natural Language Understanding">
            <p>
                Analyze text to extract meta-data from content such as concepts, entities,
                emotion, relations, sentiment and more.
            </p>
        </AccordionItem>
        <AccordionItem title="Language Translator">
            <p>
                Translate text, documents, and websites from one language to another.
                Create industry or region-specific translations via the service's
                customization capability.
            </p>
        </AccordionItem>
    </Accordion>
</Modal>
<button on:click={moveMarker} id='move-btn'>
    MOVE MARKER {Math.round($lat * 10000) / 10000}, {Math.round($lng * 10000) / 10000}
</button>

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
