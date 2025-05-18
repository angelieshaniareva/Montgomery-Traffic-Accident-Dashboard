import * as d3 from 'https://cdn.skypack.dev/d3@7';

function _1(md) {
  return md`
  <div style="color: grey; font: 13px/25.5px var(--sans-serif); text-transform: uppercase;">
    <h1 style="display: none;">Bar Chart Race</h1><a href="https://d3js.org/">D3</a> › 
    <a href="/@d3/gallery">Gallery</a>
  </div>

  # What’s Wrecking Our Roads? (2015–2024)

  The Great Circumstance Showdown — what is causing the most chaos on the roads?
  `;
}


function _data(FileAttachment) {
  return FileAttachment("accidents.csv").csv({ typed: true }).then((data) => {

    const excluded = new Set([
      "Not Applicable",
      "Unknown",
      
    ]);

    const filteredData = data
      .filter(d => d.circumstance && !excluded.has(d.circumstance)) // ❌ Exclude unwanted
      .map(d => ({
        circumstance: d.circumstance,
        crash_date_time: d.crash_date_time
      }));

    return filteredData;
  });
}



function _replay(html) {
  return html`<button class="start-button">▶ Start Animation</button>`;
}


async function* _chart(viewof_replay, d3, width, height, bars, axis, labels, ticker, keyframes, duration, x, invalidation) {
  // Create SVG
  const svg = d3.create("svg")
    .attr("viewBox", [0, 0, width, height])
    .attr("width", width)
    .attr("height", height)
    .attr("style", "max-width: 100%; height: auto;");

  const updateBars = bars(svg);
  const updateAxis = axis(svg);
  const updateLabels = labels(svg);
  const updateTicker = ticker(svg);

  // Initial static state (first frame)
  const [firstDate, firstData] = keyframes[0];
  x.domain([0, firstData[0].value]);
  updateAxis([firstDate, firstData], svg.transition().duration(0));
  updateBars([firstDate, firstData], svg.transition().duration(0));
  updateLabels([firstDate, firstData], svg.transition().duration(0));
  updateTicker([firstDate, firstData], svg.transition().duration(0));

  // Display chart before animation
  yield svg.node();

  // Wait for button click
  await new Promise(resolve => viewof_replay.onclick = resolve);

  // Start animation loop
  for (const keyframe of keyframes) {
    const transition = svg.transition().duration(duration).ease(d3.easeLinear);

    x.domain([0, keyframe[1][0].value]);

    updateAxis(keyframe, transition);
    updateBars(keyframe, transition);
    updateLabels(keyframe, transition);
    updateTicker(keyframe, transition);

    invalidation.then(() => svg.interrupt());
    await transition.end();
  }
}


function _duration() { return 100; }

function _n() { return 10; }

function _names(data) {
  return new Set(data.map(d => d.circumstance));
}

function _datevalues(d3, data) {
  const start = new Date("2015-01-31");
  const maxDataDate = d3.max(data, d => new Date(d.crash_date_time));
  const cutoffDate = new Date("2024-12-31");
  const end = d3.min([maxDataDate, cutoffDate]); // ensures we don’t go past cutoff

  // Create full list of months between start and end
  const months = [];
  let current = new Date(start);
  current.setDate(1); // normalize to 1st
  while (current <= end) {
    months.push(new Date(current));
    current.setMonth(current.getMonth() + 1);
  }

  // Get all unique categories
  const allCircumstances = Array.from(new Set(data.map(d => d.circumstance)));

  // Initialize cumulative map
  const cumulative = new Map();
  const results = [];

  for (const month of months) {
    const monthStr = `${month.getFullYear()}-${String(month.getMonth() + 1).padStart(2, '0')}`;
    const monthData = data.filter(d => {
      const date = new Date(d.crash_date_time);
      return date.getFullYear() === month.getFullYear() && date.getMonth() === month.getMonth();
    });

    const monthlyCounts = d3.rollup(
      monthData,
      v => v.length,
      d => d.circumstance
    );

    // Update cumulative values
    for (const name of allCircumstances) {
      cumulative.set(name, (cumulative.get(name) || 0) + (monthlyCounts.get(name) || 0));
    }

    // Push the result at the **end of the month**
    results.push([
      new Date(month.getFullYear(), month.getMonth() + 1, 0),
      new Map(cumulative)
    ]);
  }

  return results;
}




function _rank(names, d3, n) {
  function rank(value) {
    const data = Array.from(names, name => ({ name, value: value(name) })); // ✅ use `name`
    data.sort((a, b) => d3.descending(a.value, b.value));
    for (let i = 0; i < data.length; ++i) data[i].rank = Math.min(n, i);
    return data;
  }
  return rank;
}


function _k() { return 4; }

function _keyframes(d3, datevalues, k, rank) {
  const keyframes = [];
  console.log('Datevalues:', datevalues); // Debug
  
  for (const [[ka, a], [kb, b]] of d3.pairs(datevalues)) {
    for (let i = 0; i < k; ++i) {
      const t = i / k;
      keyframes.push([
        new Date(ka.getTime() * (1 - t) + kb.getTime() * t),
        rank(name => (a.get(name) || 0) * (1 - t) + (b.get(name) || 0) * t)
      ]);
    }
  }
  
  keyframes.push([datevalues[datevalues.length - 1][0], rank(name => datevalues[datevalues.length - 1][1].get(name) || 0)]);
  console.log('Keyframes:', keyframes); // Debug
  return keyframes;
}

function _nameframes(d3, keyframes) {
  return d3.groups(keyframes.flatMap(([, data]) => data), d => d.name);
}

function _prev(nameframes, d3) {
  return new Map(nameframes.flatMap(([, data]) => d3.pairs(data, (a, b) => [b, a])));
}

function _next(nameframes, d3) {
  return new Map(nameframes.flatMap(([, data]) => d3.pairs(data)));
}

function _bars(n, color, y, x, prev, next) {
  return function (svg) {
    let bar = svg.append("g")
      .attr("fill-opacity", 0.6)
      .selectAll("rect");

    return ([date, data], transition) => {
      if (!data || !Array.isArray(data)) {
        console.error("Invalid data:", data);
        return;
      }

      return bar = bar
        .data(data.slice(0, n), d => d.name) // ✅ FIXED
        .join(
          enter => enter.append("rect")
            .attr("fill", color)
            .attr("height", y.bandwidth())
            .attr("x", x(0))
            .attr("y", d => y((prev.get(d) || d).rank))
            .attr("width", d => x((prev.get(d) || d).value) - x(0)),
          update => update,
          exit => exit.transition(transition).remove()
            .attr("y", d => y((next.get(d) || d).rank))
            .attr("width", d => x((next.get(d) || d).value) - x(0))
        )
        .call(bar => bar.transition(transition)
          .attr("y", d => y(d.rank))
          .attr("width", d => x(d.value) - x(0)));
    };
  };
}

function _labels(n, x, prev, y, next, textTween) {
  return function (svg) {
    let label = svg.append("g")
      .style("font", "bold 12px var(--sans-serif)")
      .style("font-variant-numeric", "tabular-nums")
      .attr("text-anchor", "start")
      .selectAll("text");

    return ([date, data], transition) => {
      if (!data || !Array.isArray(data)) {
        console.error("Invalid data:", data);
        return;
      }

      return label = label
        .data(data.slice(0, n), d => d.name) // ✅ FIXED
        .join(
          enter => enter.append("text")
            .attr("transform", d => `translate(${x((prev.get(d) || d).value)},${y((prev.get(d) || d).rank)})`)
            .attr("y", y.bandwidth() / 2)
            .attr("x", 6)
            .attr("dy", "-0.25em")
            .text(d => d.name) // ✅ FIXED
            .call(text => text.append("tspan")
              .attr("fill-opacity", 0.7)
              .attr("font-weight", "normal")
              .attr("x", 7)
              .attr("dy", "1.15em")
              .text(d => d.value||0)), // optional: show value
          update => update,
          exit => exit.transition(transition).remove()
            .attr("transform", d => `translate(${x((next.get(d) || d).value)},${y((next.get(d) || d).rank)})`)
            .call(g => g.select("tspan").tween("text", d => textTween(d.value, (next.get(d) || d).value)))
        )
        .call(bar => bar.transition(transition)
          .attr("transform", d => `translate(${x(d.value)},${y(d.rank)})`)
          .call(g => g.select("tspan").tween("text", d => textTween((prev.get(d) || d).value, d.value))));
    };
  };
}


function _textTween(d3, formatNumber) {
  return function textTween(a, b) {
    const i = d3.interpolateNumber(a, b);
    return function (t) {
      this.textContent = formatNumber(i(t));
    };
  };
}

function _formatNumber(d3) {
  return d3.format(",d");
}

function _tickFormat() {
  return undefined;
}

function _axis(marginTop, d3, x, width, tickFormat, barSize, n, y) {
  return function (svg) {
    const g = svg.append("g")
      .attr("transform", `translate(0,${marginTop})`);

    const axis = d3.axisTop(x)
      .ticks(width / 160, tickFormat)
      .tickSizeOuter(0)
      .tickSizeInner(-barSize * (n + y.padding()));

    return (_, transition) => {
      g.transition(transition).call(axis);
      g.select(".tick:first-of-type text").remove();
      g.selectAll(".tick:not(:first-of-type) line").attr("stroke", "white");
      g.select(".domain").remove();
    };
  };
}

function _ticker(barSize, width, marginTop, n, formatDate, keyframes) {
  return function (svg) {
    const now = svg.append("text")
      .style("font-size", `${barSize}px`)
      .style("font-weight", "bold")
      .style("font-family", "var(--sans-serif)")
      .style("font-variant-numeric", "tabular-nums")
      .attr("text-anchor", "end")
      .attr("x", width - 6)
      .attr("y", marginTop + barSize * (n - 0.45))
      .attr("dy", "0.32em")
      .text(formatDate(keyframes[0][0]));

    return ([date], transition) => {
      transition.end().then(() => now.text(formatDate(date)));
    };
  };
}

function _formatDate(d3) {
  return d3.utcFormat("%Y");
}

function _color(d3) {
  // Simplified since we don't have categories
  return d3.scaleOrdinal(d3.schemeTableau10);
}

function _x(d3, marginLeft, width, marginRight) {
  return d3.scaleLinear([0, 1], [marginLeft, width - marginRight]);
}

function _y(d3, n, marginTop, barSize) {
  return d3.scaleBand()
    .domain(d3.range(n + 1))
    .rangeRound([marginTop, marginTop + barSize * (n + 1 + 0.1)])
    .padding(0.1);
}

function _height(marginTop, barSize, n, marginBottom) {
  return marginTop + barSize * n + marginBottom;
}

function _barSize() { return 48; }

function _marginTop() { return 16; }

function _marginRight() { return 200; }

function _marginBottom() { return 6; }

function _marginLeft() { return 0; }

export default function define(runtime, observer) {
  const main = runtime.module();
  function toString() { return this.url; }
  const fileAttachments = new Map([
    ["accidents.csv", { url: new URL("./files/final_modified_file.csv", import.meta.url), mimeType: "text/csv", toString }]
  ]);
  main.builtin("FileAttachment", runtime.fileAttachments(name => fileAttachments.get(name)));
  main.variable(observer()).define(["md"], _1);
  main.variable(observer("data")).define("data", ["FileAttachment"], _data);
  main.variable(observer("viewof replay")).define("viewof replay", ["html"], _replay);
  main.variable(observer("replay")).define("replay", ["Generators", "viewof replay"], (G, _) => G.input(_));
  main.variable(observer("chart")).define("chart", ["viewof replay", "d3", "width", "height", "bars", "axis", "labels", "ticker", "keyframes", "duration", "x", "invalidation"], _chart);
  main.variable(observer("duration")).define("duration", _duration);
  main.variable(observer("n")).define("n", _n);
  main.variable(observer("names")).define("names", ["data"], _names);
  main.variable(observer("datevalues")).define("datevalues", ["d3", "data"], _datevalues);
  main.variable(observer("rank")).define("rank", ["names", "d3", "n"], _rank);
  main.variable(observer("k")).define("k", _k);
  main.variable(observer("keyframes")).define("keyframes", ["d3", "datevalues", "k", "rank"], _keyframes);
  main.variable(observer("nameframes")).define("nameframes", ["d3", "keyframes"], _nameframes);
  main.variable(observer("prev")).define("prev", ["nameframes", "d3"], _prev);
  main.variable(observer("next")).define("next", ["nameframes", "d3"], _next);
  main.variable(observer("bars")).define("bars", ["n", "color", "y", "x", "prev", "next"], _bars);
  main.variable(observer("labels")).define("labels", ["n", "x", "prev", "y", "next", "textTween"], _labels);
  main.variable(observer("textTween")).define("textTween", ["d3", "formatNumber"], _textTween);
  main.variable(observer("formatNumber")).define("formatNumber", ["d3"], _formatNumber);
  main.variable(observer("tickFormat")).define("tickFormat", _tickFormat);
  main.variable(observer("axis")).define("axis", ["marginTop", "d3", "x", "width", "tickFormat", "barSize", "n", "y"], _axis);
  main.variable(observer("ticker")).define("ticker", ["barSize", "width", "marginTop", "n", "formatDate", "keyframes"], _ticker);
  main.variable(observer("formatDate")).define("formatDate", ["d3"], _formatDate);
  main.variable(observer("color")).define("color", ["d3", "data"], _color);
  main.variable(observer("x")).define("x", ["d3", "marginLeft", "width", "marginRight"], _x);
  main.variable(observer("y")).define("y", ["d3", "n", "marginTop", "barSize"], _y);
  main.variable(observer("height")).define("height", ["marginTop", "barSize", "n", "marginBottom"], _height);
  main.variable(observer("barSize")).define("barSize", _barSize);
  main.variable(observer("marginTop")).define("marginTop", _marginTop);
  main.variable(observer("marginRight")).define("marginRight", _marginRight);
  main.variable(observer("marginBottom")).define("marginBottom", _marginBottom);
  main.variable(observer("marginLeft")).define("marginLeft", _marginLeft);
  return main;
}
