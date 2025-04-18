use crate::accumulator::manager::{AccumulatorConfig, PhasorAccumulatorConfig};

use crate::ieee_c37_118::config::ConfigurationFrame;
use crate::ieee_c37_118::phasors::PhasorType;

use crate::ieee_c37_118::units::MeasurementType;

use arrow::datatypes::DataType;

pub fn config_to_accumulators(
    config: &ConfigurationFrame,
    output_phasor_type: Option<PhasorType>,
) -> (Vec<AccumulatorConfig>, Vec<PhasorAccumulatorConfig>) {
    // Takes a IEEE c37.118 configuration frame struct and converts it to vectors of accumulator configs

    // Start with the timestamp accumulator
    let mut accumulators: Vec<AccumulatorConfig> = vec![AccumulatorConfig {
        var_loc: 6,
        var_len: 8,
        var_type: DataType::Int64,
        name: "DATETIME".to_string(),
    }];

    // Create vector for phasor accumulators
    let mut phasor_accumulators: Vec<PhasorAccumulatorConfig> = vec![];

    // Process each PMU configuration
    let mut current_offset = 14 + 2; // Prefix + STAT

    match output_phasor_type {
        Some(value) => println!("Converting phasor data to: {}", value),
        None => println!("Raw Phasor data is being accumulated. Integer phasor data must be scaled according to PHUNIT/10E5"),
    }
    for pmu_config in &config.pmu_configs {
        let station_name = String::from_utf8_lossy(&pmu_config.stn).trim().to_string();

        // Process phasors
        if pmu_config.phnmr > 0 {
            let column_names = pmu_config.get_column_names();
            let phasor_column_names = column_names.iter().take(pmu_config.phnmr as usize);

            // Determine phasor type from format field
            let input_phasor_type = match (
                pmu_config.format & 0x0002 != 0, // Float flag
                pmu_config.format & 0x0001 != 0, // Polar flag
            ) {
                (false, false) => PhasorType::IntRect,
                (false, true) => PhasorType::IntPolar,
                (true, false) => PhasorType::FloatRect,
                (true, true) => PhasorType::FloatPolar,
            };

            for (i, name) in phasor_column_names.enumerate() {
                // Get the appropriate scale factor from phunit
                let scale_factor = if i < pmu_config.phunit.len() {
                    pmu_config.phunit[i]._scale_factor
                } else {
                    1 // Default scale factor if not specified
                };

                let is_current = if i < pmu_config.phunit.len() {
                    pmu_config.phunit[i].is_current
                } else {
                    false
                };

                // Determine proper name with type
                let unit_type = if is_current { "Current" } else { "Voltage" };
                let formatted_name = format!("{} ({})", name, unit_type);

                // Use the specified output type or default to input type
                let output_type = output_phasor_type.unwrap_or(input_phasor_type);

                // Get the phasor size
                let phasor_size = pmu_config.phasor_size();

                // Create component names based on output type
                let (name_real, name_imag) = match output_type {
                    // For polar representation
                    //
                    PhasorType::FloatPolar | PhasorType::IntPolar => (
                        format!("{}_magnitude", formatted_name),
                        format!("{}_angle", formatted_name),
                    ),
                    // For rectangular representation
                    PhasorType::FloatRect | PhasorType::IntRect => (
                        format!("{}_real", formatted_name),
                        format!("{}_imag", formatted_name),
                    ),
                };

                // Create PhasorAccumulatorConfig instead of struct type
                phasor_accumulators.push(PhasorAccumulatorConfig {
                    var_loc: current_offset as u16,
                    input_type: input_phasor_type,
                    output_type,
                    scale_factor,
                    name_real,
                    name_imag,
                });

                current_offset += phasor_size;
            }
        }

        // Process frequency
        let freq_size = pmu_config.freq_dfreq_size();
        let freq_type = if pmu_config.format & 0x0008 != 0 {
            DataType::Float32
        } else {
            DataType::Int16
        };

        accumulators.push(AccumulatorConfig {
            var_loc: current_offset as u16,
            var_len: freq_size as u8,
            var_type: freq_type.clone(),
            name: format!("{}_{}_FREQ (Frequency)", station_name, pmu_config.idcode),
        });
        current_offset += freq_size;

        // Process ROCOF (dfreq)
        accumulators.push(AccumulatorConfig {
            var_loc: current_offset as u16,
            var_len: freq_size as u8,
            var_type: freq_type,
            name: format!("{}_{}_DFREQ (ROCOF)", station_name, pmu_config.idcode),
        });
        current_offset += freq_size;

        // Process analog values
        if pmu_config.annmr > 0 {
            let column_names = pmu_config.get_column_names();
            let analog_column_names = column_names
                .iter()
                .skip(pmu_config.phnmr as usize)
                .take(pmu_config.annmr as usize);

            let analog_size = pmu_config.analog_size();
            let analog_type = if pmu_config.format & 0x0004 != 0 {
                DataType::Float32
            } else {
                DataType::Int16
            };

            for (i, name) in analog_column_names.enumerate() {
                // Get measurement type from anunit if available
                let measurement_type = if i < pmu_config.anunit.len() {
                    match &pmu_config.anunit[i].measurement_type {
                        MeasurementType::SinglePointOnWave => "Single Point-On-Wave",
                        MeasurementType::RmsOfAnalogInput => "RMS",
                        MeasurementType::PeakOfAnalogInput => "Peak",
                        MeasurementType::Reserved(code) => &format!("Reserved ({})", code),
                    }
                } else {
                    "Analog" // Default if not specified
                };

                accumulators.push(AccumulatorConfig {
                    var_loc: current_offset as u16,
                    var_len: analog_size as u8,
                    var_type: analog_type.clone(),
                    name: format!("{} ({})", name, measurement_type),
                });
                current_offset += analog_size;
            }
        }

        // Process digital values
        if pmu_config.dgnmr > 0 {
            let column_names = pmu_config.get_column_names();
            let digital_column_names = column_names
                .iter()
                .skip((pmu_config.phnmr + pmu_config.annmr) as usize)
                .take(pmu_config.dgnmr as usize);

            for name in digital_column_names {
                accumulators.push(AccumulatorConfig {
                    var_loc: current_offset as u16,
                    var_len: 2, // Digital values are always 2 bytes
                    var_type: DataType::UInt16,
                    name: format!("{} (Digital)", name),
                });
                current_offset += 2;
            }
        }
    }

    (accumulators, phasor_accumulators)
}
