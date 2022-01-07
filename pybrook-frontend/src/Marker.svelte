<script>
	import { onMount } from 'svelte';
	export let marker = undefined;
	export let map, lat, lng;

	import { createEventDispatcher } from 'svelte'
    import {tweened} from "svelte/motion";
    import {cubicOut} from "svelte/easing";
	const dispatch = createEventDispatcher();
	let prevLatLng = undefined;
	let destroy;
	let markerProto;

    let posAnim = (initial) => tweened(initial, {
        duration: 1000,
        easing: cubicOut
    });
    let latAnim = posAnim(lat);
    let lngAnim = posAnim(lng);
    $: latAnim.set(lat);
    $: lngAnim.set(lng);
	$: {
        if(marker) {
            marker.setLatLng([$latAnim, $lngAnim]);
        }

	}
	
	function createMarker(markerElement){
        console.log('create marker')
		marker = L.marker([$latAnim, $lngAnim]).addTo(map);
		let destroy = () => {
			marker.remove();
			marker = undefined;
		}
        marker.on('click', () => dispatch('click'))
		if(marker.start !== undefined)
		marker.start();
		return {
			destroy
		}
	}
</script>
<div use:createMarker></div>
