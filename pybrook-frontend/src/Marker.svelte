<script>
	import { onMount } from 'svelte';
	export let marker = undefined;
	export let map, lat, lng;

	import { createEventDispatcher } from 'svelte'
	const dispatch = createEventDispatcher();
	let prevLatLng = undefined;
	let destroy;
	let markerProto;
	
	$: latLng = [lat, lng];
	$: {
		if(prevLatLng !== undefined && prevLatLng !== latLng){
			marker.setLatLng(latLng);
		}	
		prevLatLng = latLng;
	}
	
	function createMarker(markerElement){
		marker = L.marker(latLng).addTo(map);
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
