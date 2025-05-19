function _chart(d3, speedData, mode, selectedYear1, selectedYear2) {
  const width = 960;
  const height = 500;
  const marginTop = 50;
  const marginRight = 20;
  const marginBottom = 30;
  const marginLeft = 40;

  // Filter data based on selected mode
  let filteredData;
  let chartTitle;
  
  if (mode === "total") {
    filteredData = speedData;
    chartTitle = "Number of Accidents Across Speed Limits (All Years)";
  } else {
    // For compare mode, create two separate datasets
    // Convert selectedYear values to string to ensure proper comparison
    const year1Str = String(selectedYear1);
    const year2Str = String(selectedYear2);
    
    const year1Data = speedData.filter(d => String(d.year) === year1Str);
    const year2Data = speedData.filter(d => String(d.year) === year2Str);
    
    filteredData = [
      ...year1Data.map(d => ({...d, yearGroup: year1Str})),
      ...year2Data.map(d => ({...d, yearGroup: year2Str}))
    ];
    
    chartTitle = `Number of Accidents Across Speed Limits: ${year1Str} vs ${year2Str}`;
  }

  // Create SVG container
  const svg = d3.create("svg")
    .attr("width", width)
    .attr("height", height)
    .attr("viewBox", [0, 0, width, height])
    .attr("style", "max-width: 100%; height: auto;");
  
  // Add title
  svg.append("text")
    .attr("x", width / 2)
    .attr("y", marginTop / 2)
    .attr("text-anchor", "middle")
    .attr("font-size", "16px")
    .attr("font-weight", "bold")
    .text(chartTitle);

  if (mode === "total") {
    // Count occurrences of each exact speed limit
    const speedCounts = Array.from(
      d3.rollup(filteredData, v => v.length, d => d.speed_limit),
      ([speed, count]) => ({ speed: +speed, count })
    ).sort((a, b) => a.speed - b.speed);

    const x = d3.scaleBand()
      .domain(speedCounts.map(d => d.speed))
      .range([marginLeft, width - marginRight])
      .padding(0.1);

    const y = d3.scaleLinear()
      .domain([0, d3.max(speedCounts, d => d.count)])
      .nice()
      .range([height - marginBottom, marginTop]);

    // Bars
    svg.append("g")
      .attr("fill", "steelblue")
      .selectAll("rect")
      .data(speedCounts)
      .join("rect")
        .attr("x", d => x(d.speed))
        .attr("width", x.bandwidth())
        .attr("y", d => y(d.count))
        .attr("height", d => y(0) - y(d.count));

    // Labels
    svg.append("g")
      .selectAll("text")
      .data(speedCounts)
      .join("text")
        .attr("x", d => x(d.speed) + x.bandwidth() / 2)
        .attr("y", d => y(d.count) - 5)
        .attr("text-anchor", "middle")
        .attr("font-size", "10px")
        .attr("fill", "black")
        .text(d => d.count);

    // X-axis
    svg.append("g")
      .attr("transform", `translate(0,${height - marginBottom})`)
      .call(d3.axisBottom(x).tickFormat(d => `${d}`))
      .call(g => g.append("text")
        .attr("x", width)
        .attr("y", marginBottom - 4)
        .attr("fill", "currentColor")
        .attr("text-anchor", "end")
        .text("Speed Limit (mph) →"));

    // Y-axis
    svg.append("g")
      .attr("transform", `translate(${marginLeft},0)`)
      .call(d3.axisLeft(y).ticks(height / 40))
      .call(g => g.select(".domain").remove())
      .call(g => g.append("text")
        .attr("x", -marginLeft)
        .attr("y", 10)
        .attr("fill", "currentColor")
        .attr("text-anchor", "start")
        .text("↑ Frequency"));
  } else {
    // Ensure we have string values for years
    const year1Str = String(selectedYear1);
    const year2Str = String(selectedYear2);
    
    // For comparison mode
    // Group data by speed limit and year
    const nestedData = Array.from(
      d3.rollup(filteredData, v => v.length, d => d.yearGroup, d => d.speed_limit),
      ([year, speeds]) => ({
        year,
        speeds: Array.from(speeds, ([speed, count]) => ({ speed: +speed, count }))
          .sort((a, b) => a.speed - b.speed)
      })
    );

    // Get all unique speed limits
    const allSpeeds = Array.from(
      new Set(filteredData.map(d => d.speed_limit))
    ).sort((a, b) => a - b);

    // Create scales
    const x0 = d3.scaleBand()
      .domain(allSpeeds)
      .range([marginLeft, width - marginRight])
      .padding(0.1);
    
    const x1 = d3.scaleBand()
      .domain([year1Str, year2Str])
      .range([0, x0.bandwidth()])
      .padding(0.05);
    
    const maxCount = d3.max(
      nestedData.flatMap(yearData => 
        yearData.speeds.map(d => d.count)
      )
    );
    
    const y = d3.scaleLinear()
      .domain([0, maxCount])
      .nice()
      .range([height - marginBottom, marginTop]);
    
    // Color scale for the years
    const color = d3.scaleOrdinal()
      .domain([year1Str, year2Str])
      .range(["steelblue", "orange"]);
    
    // Create grouped bars
    nestedData.forEach(yearData => {
      // For each speed value, create a group
      allSpeeds.forEach(speed => {
        // Find the data for this speed in the current year, or use 0
        const speedData = yearData.speeds.find(d => d.speed === speed) || { speed, count: 0 };
        
        svg.append("rect")
          .attr("x", x0(speed) + x1(yearData.year))
          .attr("y", y(speedData.count))
          .attr("width", x1.bandwidth())
          .attr("height", y(0) - y(speedData.count))
          .attr("fill", color(yearData.year));
        
        // Add label if count is not zero
        
        svg.append("text")
          .attr("x", x0(speed) + x1(yearData.year) + x1.bandwidth() / 2)
          .attr("y", y(speedData.count) - 5)
          .attr("text-anchor", "middle")
          .attr("font-size", "8px")
          .attr("fill", "black")
          .text(speedData.count);
      
      });
    });
    
    // X-axis
    svg.append("g")
      .attr("transform", `translate(0,${height - marginBottom})`)
      .call(d3.axisBottom(x0).tickFormat(d => `${d}`))
      .call(g => g.append("text")
        .attr("x", width)
        .attr("y", marginBottom - 4)
        .attr("fill", "currentColor")
        .attr("text-anchor", "end")
        .text("Speed Limit (mph) →"));
    
    // Y-axis
    svg.append("g")
      .attr("transform", `translate(${marginLeft},0)`)
      .call(d3.axisLeft(y).ticks(height / 40))
      .call(g => g.select(".domain").remove())
      .call(g => g.append("text")
        .attr("x", -marginLeft)
        .attr("y", 10)
        .attr("fill", "currentColor")
        .attr("text-anchor", "start")
        .text("↑ Frequency"));
    
    // Add legend
    const legend = svg.append("g")
      .attr("transform", `translate(${width - marginRight - 100}, ${marginTop})`);
    
    [year1Str, year2Str].forEach((year, i) => {
      legend.append("rect")
        .attr("x", 0)
        .attr("y", i * 20)
        .attr("width", 15)
        .attr("height", 15)
        .attr("fill", color(year));
      
      legend.append("text")
        .attr("x", 20)
        .attr("y", i * 20 + 12)
        .attr("font-size", "12px")
        .text(year);
    });
  }

  return svg.node();
}

// Add controls for mode selection and year selection
function _mode(Inputs) {
  return Inputs.radio(["total", "compare"], {label: "Display Mode", value: "total"});
}

function _years(speedData) {
  return Array.from(new Set(speedData.map(d => d.year)))
    .map(year => parseInt(year))
    .filter(year => !isNaN(year) && year >= 2015 && year <= 2024)
    .sort((a, b) => a - b);
}


function _selectedYear1(Inputs, years) {
  return Inputs.select(years, {label: "First Year", value: years[0]});
}

function _selectedYear2(Inputs, years) {
  return Inputs.select(years, {label: "Second Year", value: years[years.length - 1]});
}

function _speedData(FileAttachment) {
  return FileAttachment("speed_limit.csv").csv({ typed: true }).then(data => {
    const cleaned = data.map(d => {
      const yearValue = d.year ? String(d.year).trim() : null;
      return {
        ...d,
        year: yearValue,
        speed_limit: +d.speed_limit // ensure it's a number
      };
    });

    return cleaned.filter(d =>
      d.year !== null &&
      !isNaN(parseInt(d.year)) &&
      d.speed_limit !== 0
    );
  });
}



export default function define(runtime, observer) {
  const main = runtime.module();
  function toString() { return this.url; }
  const fileAttachments = new Map([
    ["speed_limit.csv", {url: new URL("./files/final_modified_file.csv", import.meta.url), mimeType: "text/csv", toString}]
  ]);
  main.builtin("FileAttachment", runtime.fileAttachments(name => fileAttachments.get(name)));

  main.variable(observer("mode")).define("mode", ["Inputs"], _mode);
  main.variable(observer("years")).define("years", ["speedData"], _years);
  main.variable(observer("selectedYear1")).define("selectedYear1", ["Inputs", "years"], _selectedYear1);
  main.variable(observer("selectedYear2")).define("selectedYear2", ["Inputs", "years"], _selectedYear2);
  main.variable(observer("chart")).define("chart", ["d3", "speedData", "mode", "selectedYear1", "selectedYear2"], _chart);
  main.variable().define("speedData", ["FileAttachment"], _speedData);

  return main;
}