<script>
    import L from 'leaflet';
    import {createEventDispatcher, onMount} from 'svelte';
    import {Accordion, AccordionItem} from "carbon-components-svelte";
    import 'leaflet/dist/leaflet.css';

    delete L.Icon.Default.prototype._getIconUrl;
    import iconRetinaUrl from 'leaflet/dist/images/marker-icon-2x.png';
    import iconUrl from 'leaflet/dist/images/marker-icon.png';
    import shadowUrl from 'leaflet/dist/images/marker-shadow.png';

    let map;
    const dispatch = createEventDispatcher();

    function createLeaflet(node) {
		L.Icon.Default.mergeOptions({
			iconRetinaUrl,
			iconUrl,
			shadowUrl
		})
        map = L.map(node).setView([52.25, 20.9], 13).on('zoom', (e) => dispatch('zoom', e));
        map.on('moveend', function(e) {
            dispatch('moveend', {bounds: map.getBounds()})
        });
        L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }).addTo(map);
        return {
            destroy() {
                map.remove();
                map = undefined;
            },
        };
    }
</script>


<div class="leaflet-map" use:createLeaflet>
    {#if map}
        <slot {map}/>
    {/if}
</div>

<style type="text/css">
    .leaflet-map {
        width: 100%;
        height: calc(100vh - 40px);
    }
</style>
