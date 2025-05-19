document.addEventListener('DOMContentLoaded', () => {
    initializeApp();
});

// Visualization registry - stores metadata and render functions for each visualization
const visualizationRegistry = {
    'accident-circumstances': {
        title: "What's Wrecking Our Roads? (2015–2024)",
        description: "The Great Circumstance Showdown — what is causing the most chaos on the roads?",
        details: `
            <h3>About This Visualization</h3>
            <p>This bar chart race shows the cumulative number of traffic accidents by their attributed circumstances from 2015 to 2024. The animation reveals which factors have consistently been the most dangerous on our roads and how their relative impact has changed over time.</p>
           
        `,
        renderFunction: renderAccidentCircumstances,

    },
    'traffic-volume': {
        title: "Fast, Furious... and Frequently Crashed (2015–2024)",
        description: "From slow zones to high-speed stretches, the chart shows which speed limits see the most action — and potentially, the most trouble.",
        details: `
            <h3>About This Visualization</h3>
            <p>This interactive histogram highlights the most common speed limits associated with road incidents from 2015 to 2024.</p>
            <p>Use the "Compare" mode to explore how the frequency of incidents at different speed limits has changed between any two years — and spot shifts or patterns in crash distribution over time.</p>
           
        `,
        renderFunction: renderSpeedLimit,
        
    },
    'accident-map': {
        title: "Where It Hits Hardest (2015–2024)",
        description: "See the crash patterns unfold across Montgomery County — hour by hour, dot by dot.",
        details: `
            <h3>About This Visualization</h3>
            <p>This animated heatmap shows the hourly geographic distribution of traffic accidents across Montgomery County from 2015 to 2024. Each dot represents a crash location, pulsing according to the time of day it occurred.</p>
          
        `,
        renderFunction: renderAccidentMap,
        
    },

    'when-dashboard': {
        title: "When are Traffic Accidents Most Frequent? Trends & High-Risk Periods",
        description: "Who knew rush hour could be so... crashy? This dashboard dives into when traffic accidents strike hardest — whether it's sleepy mornings, chaotic evenings, or even those deceptively peaceful holidays. Spoiler: no day is truly safe.",
        details: `
            <h3>About This Visualization</h3>
            <p>This dashboard shows the yearly trend of traffic accidents, highlights high-risk time periods across weekdays and weekends, and compares average accident rates on holidays versus regular days.</p>
<p>To explore a specific year in greater detail, click on a year along the x-axis of the "Yearly Trend of Traffic Accidents" chart. This will filter the entire dashboard accordingly.</p>
          
        `,
        renderFunction: renderWhenDashboard,
        isTableau: true 
        
    },

    'how-dashboard': {
        title: "How Severe are Traffic Accidents? Exploring Injury Patterns and Causes",
        description: "From slippery roads to suspiciously sober drivers — this dashboard spills the tea on what really makes traffic accidents worse.",
        details: `
            <h3>About This Visualization</h3>
            <p>This dashboard explores how various environmental conditions, substance abuse factors, and safety measures impact the severity of traffic accidents.</p>
            <p>Filters are available to narrow down the data by year, month, and injury severity for a more focused analysis.</p>
  
          
        `,
        renderFunction: renderHowDashboard,
        isTableau: true 
        
    },
    'where-dashboard': {
        title: "Where Do Injuries and Substance Related Traffic Incidents Happen?",
        description: "Bar charts, heatmaps, and hotspots — this dashboard breaks down where things go wrong, from booze-fueled crashes to dangerous intersections.",
        details: `
            <h3>About This Visualization</h3>
          <p>This dashboard maps injury-related crash locations across Montgomery County, categorizing incidents by road name, intersection type, and injury severity.</p>
        <p>It also includes a focused view on traffic incidents involving substance use, such as alcohol or drugs, to reveal potential overlap between impaired driving and high-risk zones.</p>
        <p>Use the filters to adjust by injury type or substance category and explore how geography, intersection layouts, and substance involvement shape traffic danger zones.</p>
  
          
        `,
        renderFunction: renderWhereDashboard,
        isTableau: true 
        
    }

};

function initializeApp() {
    // Set up event listeners
    setupNavigationListeners();
    setupVisualizationButtons();
    
    // Check URL hash on page load
    handleUrlHash();
    
    // Listen for hash changes
    window.addEventListener('hashchange', handleUrlHash);
}

function setupNavigationListeners() {
    // Home navigation
    document.querySelector('.nav-home').addEventListener('click', (e) => {
        e.preventDefault();
        showHomepage();
    });
    
    // Back button
    document.querySelector('.back-btn').addEventListener('click', (e) => {
        e.preventDefault();
        showHomepage();
    });
}

function setupVisualizationButtons() {
    // Add click event to all visualization buttons
    document.querySelectorAll('.viz-button').forEach(button => {
        button.addEventListener('click', () => {
            const vizId = button.getAttribute('data-viz');
            showVisualization(vizId);
        });
    });
}

function handleUrlHash() {
    const hash = window.location.hash.substring(1);
    if (hash && visualizationRegistry[hash]) {
        showVisualization(hash);
    } else {
        showHomepage();
    }
}

function showHomepage() {
    // Show homepage, hide visualization page
    document.getElementById('homepage').style.display = 'block';
    document.getElementById('graph-page').style.display = 'none';
    
    // Update URL hash
    window.location.hash = '';
}

function showVisualization(vizId) {
    if (!visualizationRegistry[vizId]) {
        console.error(`Visualization '${vizId}' not found in registry`);
        return;
    }
    
    // Hide homepage, show visualization page
    document.getElementById('homepage').style.display = 'none';
    document.getElementById('graph-page').style.display = 'block';
    
    // Update page content with visualization metadata
    const vizInfo = visualizationRegistry[vizId];
    document.getElementById('graph-title').textContent = vizInfo.title;
    document.getElementById('graph-description').textContent = vizInfo.description;
    document.getElementById('graph-details').innerHTML = vizInfo.details;
    
    // Clear previous visualization and controls
    const vizContainer = document.getElementById('visualization-container');
    const controlsContainer = document.getElementById('controls-container');
    // If it's not a Tableau dashboard, show loading spinner
    // Clear containers
    if (vizInfo.isTableau) {
        vizContainer.innerHTML = '';       // Don't add your custom loader
        controlsContainer.innerHTML = '';
    } else {
        // Add your custom loading spinner for non-Tableau charts
        vizContainer.innerHTML = `
            <div id="loader" class="chart-loader">
                <div class="spinner"></div>
                <p>Loading chart data...</p>
            </div>
        `;
        controlsContainer.innerHTML = '';
        }


    
    // Render the visualization and its controls
    vizInfo.renderFunction(vizContainer);
    if (vizInfo.controlsFunction) {
        vizInfo.controlsFunction(controlsContainer);
    }
    
    // Update URL hash
    window.location.hash = vizId;
    
    // Scroll to top
    window.scrollTo(0, 0);
}

// ==================
// visualizations/accidentCircumstances.js
// ==================
function renderAccidentCircumstances(container) {
    container.innerHTML = ''; // ✅ clears previous loaders or charts
    // Add loader first
    const loader = document.createElement('div');
    loader.id = 'loader';
    loader.className = 'chart-loader'; // use class instead of inline styles

    const spinner = document.createElement('div');
    spinner.className = 'spinner';

    const loadingText = document.createElement('p');
    loadingText.textContent = 'Loading chart data...';

    loader.appendChild(spinner);
    loader.appendChild(loadingText);
    container.appendChild(loader);


    // Create containers for chart and controls
    const controlsDiv = document.createElement('div');
    controlsDiv.id = 'controls';
    controlsDiv.style.display = 'none';

    const visDiv = document.createElement('div');
    visDiv.id = 'vis-container';
    visDiv.style.display = 'none';

    container.appendChild(controlsDiv);
    container.appendChild(visDiv);

    // Load Observable chart
    import('./barChartRace.js').then(module => {
        import('./runtime.js').then(({ Runtime, Inspector }) => {
            const runtime = new Runtime();
            const main = runtime.module(module.default, name => {
            if (name === "viewof replay") return new Inspector(controlsDiv);
                if (name === "chart") return new Inspector(visDiv);
            });

            main.value("chart").then(() => {
                loader.style.display = "none";
                controlsDiv.style.display = "block";
                visDiv.style.display = "block";
            });
        });
    });
}



// ==================
// visualizations/trafficVolume.js
// ==================
function renderSpeedLimit(container) {
    container.innerHTML = ''; // ✅ clears previous loaders or charts
    const loader = document.createElement('div');
    loader.id = 'loader';
    loader.className = 'chart-loader';

    const spinner = document.createElement('div');
    spinner.className = 'spinner';

    const loadingText = document.createElement('p');
    loadingText.textContent = 'Loading speed limit data...';

    loader.appendChild(spinner);
    loader.appendChild(loadingText);
    container.appendChild(loader);

    const controlsDiv = document.createElement('div');
    controlsDiv.id = 'controls';
    controlsDiv.style.display = 'none'; // Start hidden

    const visDiv = document.createElement('div');
    visDiv.id = 'vis-container';

    container.appendChild(controlsDiv);
    container.appendChild(visDiv);

    fetch('./files/final_modified_file.csv')
        .then(response => response.text())
        .then(csvText => {
            const rows = csvText.trim().split('\n');
            const headers = rows[0].split(',');
            const yearIndex = headers.indexOf('year');
            const validYears = new Set();

            for (let i = 1; i < rows.length; i++) {
                const values = rows[i].split(',');
                const year = values[yearIndex]?.trim();
                const parsed = parseInt(year);
                if (!isNaN(parsed) && parsed >= 2015 && parsed <= 2024) {
                    validYears.add(parsed);
                }
            }

            const yearsList = Array.from(validYears).sort((a, b) => a - b);

            // Prepare controls DOM but don't attach yet
            const modeControl = document.createElement('div');
            modeControl.innerHTML = `
               <div class="radio-toggle-group">
                <input type="radio" id="mode-total" name="mode" value="total" checked>
                <label for="mode-total">Total</label>
                
                <input type="radio" id="mode-compare" name="mode" value="compare">
                <label for="mode-compare">Compare</label>
                </div>
            `;

            const yearSelectors = document.createElement('div');
            yearSelectors.id = 'year-selectors';
            yearSelectors.style.display = 'none';
            yearSelectors.innerHTML = `
                <div>
                    <label for="year1">First Year:</label>
                    <select id="year1">
                        ${yearsList.map(year => `<option value="${year}">${year}</option>`).join('')}
                    </select>
                </div>
                <div style="margin-top: 10px;">
                    <label for="year2">Second Year:</label>
                    <select id="year2">
                        ${yearsList.map((year, i) =>
                            `<option value="${year}" ${i === yearsList.length - 1 ? 'selected' : ''}>${year}</option>`
                        ).join('')}
                    </select>
                </div>
            `;
            function syncYearDropdowns() {
                const year1 = document.getElementById('year1');
                const year2 = document.getElementById('year2');
            
                const selected1 = year1.value;
                const selected2 = year2.value;
            
                // Enable all options first
                year1.querySelectorAll('option').forEach(opt => opt.disabled = false);
                year2.querySelectorAll('option').forEach(opt => opt.disabled = false);
            
                // Disable the selected value from the other dropdown
                year1.querySelector(`option[value="${selected2}"]`).disabled = true;
                year2.querySelector(`option[value="${selected1}"]`).disabled = true;
            }
            // Setup event listeners once
            function setupListeners() {
                
                const radioButtons = modeControl.querySelectorAll('input[type="radio"]');
                radioButtons.forEach(radio => {
                    radio.addEventListener('change', function () {
                        yearSelectors.style.display = this.value === 'compare' ? 'block' : 'none';
                        updateChart();
                    });
                });


                const yearSelects = yearSelectors.querySelectorAll('select');
                yearSelects.forEach(select => {
                    select.addEventListener('change', () => {
                        syncYearDropdowns();
                        updateChart();
                    });
                });
                
                
            }

            function updateChart() {
                loader.style.display = 'flex';
                visDiv.innerHTML = '';
                controlsDiv.style.display = 'none';

                const mode = document.querySelector('input[name="mode"]:checked')?.value || 'total';
                const year1 = document.getElementById('year1')?.value || yearsList[0];
                const year2 = document.getElementById('year2')?.value || yearsList[1];

                import('./histogram.js').then(module => {
                    import('./runtime.js').then(({ Runtime, Inspector }) => {
                        const runtime = new Runtime();
                        const main = runtime.module(module.default, name => {
                            if (name === "chart") {
                                return {
                                    pending: () => {},
                                    fulfilled: (chart) => {
                                        visDiv.appendChild(chart);
                                        loader.style.display = 'none';
                                        controlsDiv.style.display = 'block';
                                    },
                                    rejected: (error) => console.error(error)
                                };
                            }
                        });

                        main.redefine("mode", mode);
                        if (mode === "compare") {
                            main.redefine("selectedYear1", String(year1));
                            main.redefine("selectedYear2", String(year2));
                        }
                    });
                });
            }

            // Only now attach controls
            controlsDiv.appendChild(modeControl);
            controlsDiv.appendChild(yearSelectors);
            setupListeners();
            syncYearDropdowns();
            updateChart();
        })
        .catch(error => {
            console.error('Error loading data:', error);
            loader.innerHTML = `<p>Error loading data: ${error.message}</p>`;
        });
}


// ==================
// visualizations/accidentMap.js
// ==================
function renderAccidentMap(container) {
    container.innerHTML = ''; // ✅ clears previous loaders or charts
    container.innerHTML = `
      <div style="width: 100%; height: 600px; border-radius: 12px; overflow: hidden;">
        <iframe 
          src="map.html" 
          width="100%" 
          height="100%" 
          style="border: none; border-radius: 12px;">
        </iframe>
      </div>
    `;
  }
  

  // ==================
// visualizations for when dashboard
// ==================
function renderWhenDashboard(container) {
    const divId = 'vizResponsiveContainer';
  
    container.innerHTML = `
      <div class='tableauPlaceholder' id='${divId}' style='width: 100%; height: 80vh; position: relative;'>
        <noscript>
          <a href="#">
            <img alt="[WHEN] Dashboard"
              src="https://public.tableau.com/static/images/Wh/WhenareTrafficAccidentsMostFrequent/WHENDashboard/1.png"
              style="border: none" />
          </a>
        </noscript>
        <object class="tableauViz" style="width: 100%; height: 100%; display: block;">
          <param name="host_url" value="https%3A%2F%2Fpublic.tableau.com%2F" />
          <param name="embed_code_version" value="3" />
          <param name="site_root" value="" />
          <param name="name" value="WhenareTrafficAccidentsMostFrequent/WHENDashboard" />
          <param name="tabs" value="no" />
          <param name="toolbar" value="yes" />
          <param name="animate_transition" value="yes" />
          <param name="display_static_image" value="yes" />
          <param name="display_spinner" value="yes" />
          <param name="display_overlay" value="yes" />
          <param name="display_count" value="yes" />
          <param name="language" value="en-GB" />
          <param name="filter" value="publish=yes" />
        </object>
      </div>
    `;
  
    // Step 2: Load Tableau's JS API
    const script = document.createElement('script');
    script.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';
    document.body.appendChild(script);
  }
  



function renderHowDashboard(container) {
    const divId = 'vizResponsiveContainer';

    // Step 1: Inject Tableau placeholder (with correct height and display)
    container.innerHTML = `
        <div class='tableauPlaceholder' id='${divId}' style='width: 100%; height: 80vh; position: relative;'>
            <noscript>
                <a href='#'>
                    <img alt='[jovi] Dashboard'
                        src='https://public.tableau.com/static/images/Ho/HowSeverearrTrafficAccidents/joviDashboard/1.png'
                        style='border: none' />
                </a>
            </noscript>
            <object class='tableauViz' style='width: 100%; height: 100%; display: block;'>
                <param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' />
                <param name='embed_code_version' value='3' />
                <param name='site_root' value='' />
                <param name='name' value='HowSeverearrTrafficAccidents/joviDashboard' />
                <param name='tabs' value='no' />
                <param name='toolbar' value='yes' />
                <param name='animate_transition' value='yes' />
                <param name='display_static_image' value='yes' />
                <param name='display_spinner' value='yes' />
                <param name='display_overlay' value='yes' />
                <param name='display_count' value='yes' />
                <param name='language' value='en-GB' />
                <param name='filter' value='publish=yes' />
            </object>
        </div>
    `;

    // Step 2: Load Tableau's JS API
    const script = document.createElement('script');
    script.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';
    document.body.appendChild(script);
}

function renderWhereDashboard(container) {
    const divId = 'vizResponsiveContainer';

    // Step 1: Inject Tableau placeholder (with correct height and display)
    container.innerHTML = `
        <div class='tableauPlaceholder' id='${divId}' style='width: 100%; height: 80vh; position: relative;'>
            <noscript>
                <a href='#'>
                    <img alt='Dashboard 1'
                        src='https://public.tableau.com/static/images/wh/where_17440873265110/Dashboard1/1.png'
                        style='border: none' />
                </a>
            </noscript>
            <object class='tableauViz' style='width: 100%; height: 100%; display: block;'>
                <param name='host_url' value='https%3A%2F%2Fpublic.tableau.com%2F' />
                <param name='embed_code_version' value='3' />
                <param name='site_root' value='' />
                <param name='name' value='where_17440873265110/Dashboard1' />
                <param name='tabs' value='no' />
                <param name='toolbar' value='yes' />
                <param name='animate_transition' value='yes' />
                <param name='display_static_image' value='yes' />
                <param name='display_spinner' value='yes' />
                <param name='display_overlay' value='yes' />
                <param name='display_count' value='yes' />
                <param name='language' value='en-GB' />
                <param name='filter' value='publish=yes' />
            </object>
        </div>
    `;

    // Step 2: Load Tableau's JS API
    const script = document.createElement('script');
    script.src = 'https://public.tableau.com/javascripts/api/viz_v1.js';
    document.body.appendChild(script);
}

