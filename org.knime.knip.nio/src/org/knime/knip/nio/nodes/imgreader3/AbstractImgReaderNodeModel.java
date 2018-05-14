package org.knime.knip.nio.nodes.imgreader3;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.knime.core.node.BufferedDataTable;
import org.knime.core.node.BufferedDataTableHolder;
import org.knime.core.node.CanceledExecutionException;
import org.knime.core.node.ExecutionMonitor;
import org.knime.core.node.InvalidSettingsException;
import org.knime.core.node.NodeModel;
import org.knime.core.node.NodeSettingsRO;
import org.knime.core.node.NodeSettingsWO;
import org.knime.core.node.defaultnodesettings.SettingsModel;
import org.knime.core.node.defaultnodesettings.SettingsModelBoolean;
import org.knime.core.node.defaultnodesettings.SettingsModelDoubleRange;
import org.knime.core.node.defaultnodesettings.SettingsModelString;
import org.knime.core.node.port.PortType;
import org.knime.knip.core.util.EnumUtils;
import org.knime.knip.nio.nodes.imgreader3.ImgReaderSettings.ImgFactoryMode;

import net.imglib2.img.ImgFactory;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.img.planar.PlanarImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;

public abstract class AbstractImgReaderNodeModel<T extends NativeType<T> & RealType<T>> extends NodeModel
		implements BufferedDataTableHolder {

	List<SettingsModel> settingsModels = new ArrayList<>();

	protected final SettingsModelBoolean checkFileFormatModel = ImgReaderSettings.createCheckFileFormatModel();
	protected final SettingsModelBoolean isGroupFilesModel = ImgReaderSettings.createIsGroupFilesModel();
	protected final SettingsModelBoolean appendOmeXMLColModel = ImgReaderSettings.createAppendOMEXMLColModel();

	protected final SettingsModelString imgFactoryModel = ImgReaderSettings.createImgFactoryModel();
	protected final SettingsModelBoolean readAllSeriesModel = ImgReaderSettings.createReadAllSeriesModel();
	protected final SettingsModelDoubleRange seriesRangeSelectionModel = ImgReaderSettings
			.createSeriesSelectionRangeModel();

	protected final SettingsModelString metadataModeModel = ImgReaderSettings.createMetaDataModeModel();
	protected final SettingsModelBoolean readAllMetaDataModel = ImgReaderSettings.createReadAllMetaDataModel();

	private BufferedDataTable dataTable;

	protected AbstractImgReaderNodeModel(PortType[] inPortTypes, PortType[] outPortTypes) {
		super(inPortTypes, outPortTypes);

		// store settings models
		settingsModels.add(checkFileFormatModel);
		settingsModels.add(isGroupFilesModel);
		settingsModels.add(appendOmeXMLColModel);
		settingsModels.add(imgFactoryModel);
		settingsModels.add(readAllSeriesModel);
		settingsModels.add(seriesRangeSelectionModel);
		settingsModels.add(metadataModeModel);
		settingsModels.add(readAllMetaDataModel);

		// TODO Set enabled status for dialog components
	}

	/**
	 * Adds additional settingsModels to the node model These are saved and
	 * loaded.
	 * 
	 * @param additionalSettingsModels
	 *            the additional settings
	 */
	protected void addAdditionalSettingsModels(final List<SettingsModel> additionalSettingsModels) {
		additionalSettingsModels.forEach(settingsModels::add);
	}

	@Override
	protected void loadInternals(final File nodeInternDir, final ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {
		doLoadInternals(nodeInternDir, exec);
	}

	protected abstract void doLoadInternals(final File nodeInternDir, final ExecutionMonitor exec);

	@Override
	protected void saveInternals(File nodeInternDir, ExecutionMonitor exec)
			throws IOException, CanceledExecutionException {

	}

	@Override
	protected void saveSettingsTo(NodeSettingsWO settings) {
		settingsModels.forEach(s -> s.saveSettingsTo(settings));
	}

	@Override
	protected void validateSettings(NodeSettingsRO settings) throws InvalidSettingsException {
		for (SettingsModel sm : settingsModels) {
			sm.validateSettings(settings);
		}
	}

	@Override
	protected void loadValidatedSettingsFrom(NodeSettingsRO settings) throws InvalidSettingsException {
		for (SettingsModel sm : settingsModels) {
			sm.loadSettingsFrom(settings);
		}
	}

	@Override
	public BufferedDataTable[] getInternalTables() {
		return new BufferedDataTable[] { dataTable };
	}

	@Override
	public void setInternalTables(BufferedDataTable[] tables) {
		dataTable = tables[0];
	}

	@Override
	protected void reset() {
		dataTable = null;
	}

	protected ImgFactory<T> createImgFactory() {
		ImgFactoryMode factorySetting = EnumUtils.valueForName(imgFactoryModel.getStringValue(),
				ImgFactoryMode.values());
	
		ImgFactory<T> factory;
		if (factorySetting == ImgFactoryMode.PLANAR_IMG) {
			factory = new PlanarImgFactory<>();
		} else if (factorySetting == ImgFactoryMode.CELL_IMG) {
			factory = new CellImgFactory<>();
		} else if (factorySetting == ImgFactoryMode.ARRAY_IMG) {
			factory = new ArrayImgFactory<>();
		} else {
			throw new IllegalStateException("Unknonw factory type " + factorySetting.toString());
		}
		return factory;
	}

}
