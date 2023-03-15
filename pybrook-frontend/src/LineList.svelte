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
