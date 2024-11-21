// JavaScript to render the D3 visualization

// Set up SVG dimensions
const width = 600, height = 600, radius = 250;

const svg = d3.select("#chart") // Attach to the #chart div
    .append("svg")
    .attr("width", width)
    .attr("height", height);

// Create a group element to center the ring
const ringGroup = svg.append("g")
    .attr("transform", `translate(${width / 2}, ${height / 2})`);

// Render the hash ring
ringGroup.append("circle")
    .attr("r", radius)
    .style("fill", "none")
    .style("stroke", "black");

// Render some example nodes
const nodes = ["Node1", "Node2", "Node3", "Node4"];
ringGroup.selectAll(".node")
    .data(nodes)
    .enter()
    .append("circle")
    .attr("class", "node")
    .attr("r", 5) // Node size
    .attr("cx", (d, i) => radius * Math.cos(2 * Math.PI * i / nodes.length))
    .attr("cy", (d, i) => radius * Math.sin(2 * Math.PI * i / nodes.length))
    .style("fill", "blue");