<script>

    import {Accordion, AccordionItem, Link, ListItem, UnorderedList} from "carbon-components-svelte";
    import {createEventDispatcher, onDestroy} from "svelte";
    import {genericReportStore} from "./stores";

    let groupKeys = new Set();
    let groups = {};
    const dispatch = createEventDispatcher()
    let openGroups = {}
    const unsubscribe = genericReportStore.subscribe((data) => {
        if (!data) return;
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
<Accordion groupKeys>
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
