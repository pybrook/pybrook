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
    import L from 'leaflet';
    import {createEventDispatcher, onMount} from 'svelte';
    import {Accordion, AccordionItem} from "carbon-components-svelte";
    import 'leaflet/dist/leaflet.css';

    delete L.Icon.Default.prototype._getIconUrl;
    import iconRetinaUrl from 'leaflet/dist/images/marker-icon-2x.png';
    import iconUrl from 'leaflet/dist/images/marker-icon.png';
    import shadowUrl from 'leaflet/dist/images/marker-shadow.png';

    export let theme;
    let map;
    let tileLayer;
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
        tileLayer = L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
            attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
        }).addTo(map);
        return {
            destroy() {
                map.remove();
                map = undefined;
            },
        };
    }
    $: {
        if(map && theme !== 'white'){
            document.querySelectorAll('.leaflet-map-pane, .leaflet-control').forEach((e) => e.className += ' invert-colors');
        } else if (map) {
            document.querySelectorAll('.leaflet-map-pane, .leaflet-control').forEach((e) => e.className = e.className.replace(' invert-colors', ''));
        }
    }
</script>

<div class="leaflet-map" use:createLeaflet>
    <slot name="notification"></slot>
    {#if map}
        <slot {map} {theme}/>
    {/if}
</div>

<style type="text/css">
    :global(.invert-colors) {
        filter: invert(1) hue-rotate(180deg) !important;
    }
    .leaflet-map {
        width: 100%;
        height: calc(100vh - 40px - 3rem);
    }
    @media screen and (max-width: 1056px){
        .leaflet-map {
            height: 80vh;
        }
    }
    :global(.leaflet-div-icon) {
         background: rgba(0,0,0,0) !important;
         border: rgba(0,0,0,0) !important;
    }

</style>
