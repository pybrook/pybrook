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

    import {Accordion, AccordionItem, Link, ListItem, UnorderedList} from "carbon-components-svelte";
    import {createEventDispatcher, onDestroy} from "svelte";
    import {configStore, genericReportStore} from "./stores";

    let groupKeys = new Set();
    let groups = {};
    const dispatch = createEventDispatcher()
    let openGroups = {}
    $: {
        if ($configStore) {
            groups = {};
            groupKeys = new Set();
        }
    }
    const unsubscribe = genericReportStore.subscribe((data) => {
        if (!data || !data.data.group) return;
        let {vehicleId, data: {group}} = data;
        if(!groupKeys.has(group)){
            groupKeys = groupKeys.add(group)
            groups[group] = new Set();
        }
        if(!groups[group].has(vehicleId)){
            groups[group] = groups[group].add(vehicleId)
        }
    });
    onDestroy(unsubscribe)
</script>
<Accordion>
    {#each Array.from(groupKeys).sort() as group (group)}
        <AccordionItem title="{group}" bind:open={openGroups[group]}>
            {#if openGroups[group]}
                <UnorderedList>
                    {#each Array.from(groups[group]).sort() as vehicleId (vehicleId)}
                        <ListItem>
                            <Link style="cursor: pointer;"
                                  on:click={() => dispatch('vehicle-selected', {vehicleId})}>{vehicleId}</Link>
                        </ListItem>
                    {/each}
                </UnorderedList>
            {/if}
        </AccordionItem>
    {/each}
</Accordion>
