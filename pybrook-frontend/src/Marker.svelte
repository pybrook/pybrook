<script>
	export let marker = undefined;
	export let map, lat, lng, vehicleId, vehicleGroup;
    export let selected=false;

	import { createEventDispatcher } from 'svelte'
    import {tweened} from "svelte/motion";
    import {cubicOut} from "svelte/easing";
	const dispatch = createEventDispatcher();
	let prevLatLng = undefined;
    let directionDeg = 0;
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
    $: {
        if(selected && map)
            map.panTo(new L.LatLng(lat, lng))
    }
    let img;
    $: img = directionDeg ? "data:image/svg+xml;base64,PHN2ZyBoZWlnaHQ9IjIwIiB3aWR0aD0iMjAiIHZpZXdCb3g9Ii01IC01IDE0IDEwIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciPgogIDxwYXRoIGZpbGw9IiMzOGUiIGQ9Ik0tNSwtNSBMMTAsMCAtNSw1IDAsMCBaIi8+Cjwvc3ZnPgo=" : "data:image/svg+xml;base64,PHN2ZyB2ZXJzaW9uPSIxLjEiIGlkPSJMYXllcl8xIiB4bWxucz0iaHR0cDovL3d3dy53My5vcmcvMjAwMC9zdmciIHhtbG5zOnhsaW5rPSJodHRwOi8vd3d3LnczLm9yZy8xOTk5L3hsaW5rIiB4PSIwcHgiIHk9IjBweCIKCSB2aWV3Qm94PSIwIDAgNDgwIDQ4MCIgc3R5bGU9ImVuYWJsZS1iYWNrZ3JvdW5kOm5ldyAwIDAgNDgwIDQ4MDsiIHhtbDpzcGFjZT0icHJlc2VydmUiPgo8ZyB0cmFuc2Zvcm09InRyYW5zbGF0ZSgwIC01NDAuMzYpIiBmaWxsPSIjMzhlIj4KCTxnPgoJCTxnPgoJCQk8cGF0aCBkPSJNNDYwLDU3Mi4wNmgtMzEuOXYtMjEuN2MwLTUuNS00LjUtMTAtMTAtMTBINjEuOWMtNS41LDAtMTAsNC41LTEwLDEwdjIxLjdIMjBjLTUuNSwwLTEwLDQuNS0xMCwxMHY3Mi42CgkJCQljMCw1LjUsNC41LDEwLDEwLDEwczEwLTQuNSwxMC0xMHYtNjIuNmgyMS45djM1NS43YzAsNS41LDQuNSwxMCwxMCwxMGg0Mi40djUyLjZjMCw1LjUsNC41LDEwLDEwLDEwaDQxLjljNS41LDAsMTAtNC41LDEwLTEwCgkJCQl2LTUyLjZoMTQ3LjZ2NTIuNmMwLDUuNSw0LjUsMTAsMTAsMTBoNDEuOWM1LjUsMCwxMC00LjUsMTAtMTB2LTUyLjZoNDIuNGM1LjUsMCwxMC00LjUsMTAtMTB2LTM1NS43SDQ1MHY2Mi42CgkJCQljMCw1LjUsNC41LDEwLDEwLDEwczEwLTQuNSwxMC0xMHYtNzIuNkM0NzAsNTc2LjU2LDQ2NS41LDU3Mi4wNiw0NjAsNTcyLjA2eiBNNzEuOSw1NjAuMzZIMjMwdjI0MS4zSDcxLjlWNTYwLjM2egoJCQkJIE0xNDYuMiwxMDAwLjM2aC0yMS45di00Mi42aDIxLjlWMTAwMC4zNnogTTM1NS43LDEwMDAuMzZoLTIxLjl2LTQyLjZoMjEuOVYxMDAwLjM2eiBNNDA4LjEsOTM3Ljc2SDcxLjl2LTExNi4xaDMzNi4yVjkzNy43NnoKCQkJCSBNNDA4LjEsODAxLjY2SDI1MHYtMjQxLjNoMTU4LjFWODAxLjY2eiIvPgoJCQk8cGF0aCBkPSJNMTIyLjEsODgzLjc2bDMxLDEwYzEsMC4zLDIuMSwwLjUsMy4xLDAuNWM0LjIsMCw4LjEtMi43LDkuNS02LjljMS43LTUuMy0xLjEtMTAuOS02LjQtMTIuNmwtMzEtMTAKCQkJCWMtNS4zLTEuNy0xMC45LDEuMS0xMi42LDYuNFMxMTYuOCw4ODIuMDYsMTIyLjEsODgzLjc2eiIvPgoJCQk8cGF0aCBkPSJNMzIzLjgsODk0LjI2YzEsMCwyLjEtMC4xLDMuMS0wLjVsMzEtMTBjNS4yLTEuNyw4LjEtNy4zLDYuNC0xMi42Yy0xLjctNS4yLTcuMy04LjEtMTIuNi02LjRsLTMxLDEwCgkJCQljLTUuMiwxLjctOC4xLDcuMy02LjQsMTIuNkMzMTUuNyw4OTEuNTYsMzE5LjYsODk0LjI2LDMyMy44LDg5NC4yNnoiLz4KCQk8L2c+Cgk8L2c+CjwvZz4KPC9zdmc+Cg=="

    $: {
        if(marker && vehicleId) {
            let markerText = `${vehicleId}`
            let background = selected ? "rgba(255,255,255,1);z-index:1000" : "rgba(255,255,255,0.5)"
            marker.setIcon(L.divIcon({
                html: `<div style="border: 1px solid rgba(10, 10, 10, 0.5); color:black; font-weight: bold; height: 30px; border-radius: 8px;border-bottom-left-radius:0;display:flex;flex-direction: row;justify-content:space-between;align-items:center;background:${background};"><div style="margin-left:5px;width:20px;display:flex;flex-direction:row;align-items: center;"><img style="transform: rotate(${directionDeg}deg);height:15px;width:15px;" src="${img}"></div><div style="margin-right: 5px;">${markerText}</div></div><div style="position:absolute;bottom:0;left:0;width: 4px;height:4px;background: black;"></div>`,
                iconSize: [markerText.length * 6.6 + 30, 30],
                iconAnchor: [0, 30]
            }));
        }
    }
	function createMarker(markerElement){
		marker = L.marker([$latAnim, $lngAnim]).addTo(map);
		let destroy = () => {
			marker.remove();
			marker = undefined;
		}
        marker.on('click', () => {dispatch('click')})
		if(marker.start !== undefined)
		marker.start();
		return {
			destroy
		}
	}
</script>
<div use:createMarker></div>
<style>
    :global(.leaflet-div-icon) {
        background: none;
        border: none;
    }
</style>
