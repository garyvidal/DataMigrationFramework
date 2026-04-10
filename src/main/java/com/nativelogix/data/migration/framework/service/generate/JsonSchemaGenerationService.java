package com.nativelogix.data.migration.framework.service.generate;

import com.nativelogix.data.migration.framework.model.project.JsonDocumentModel;
import com.nativelogix.data.migration.framework.model.project.Project;
import com.nativelogix.data.migration.framework.model.project.ProjectMapping;
import com.nativelogix.data.migration.framework.repository.FileSystemProjectRepository;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

/**
 * Generates a JSON Schema (Draft-07) for a project's JSON document mapping.
 *
 * <p>No database connection is required — the schema is derived entirely from
 * the project's {@link JsonDocumentModel} mapping metadata.</p>
 */
@Service
@RequiredArgsConstructor
public class JsonSchemaGenerationService {

    private final FileSystemProjectRepository projectRepository;
    private final JsonSchemaBuilder jsonSchemaBuilder;

    /**
     * Generates a JSON Schema string for the project identified by {@code projectId}.
     *
     * @param projectId the project UUID
     * @return JSON Schema string (Draft-07)
     * @throws IllegalArgumentException if the project is not found or has no JSON mapping
     */
    public String generateSchema(String projectId) throws Exception {
        Project project = projectRepository.findById(projectId)
                .orElseThrow(() -> new IllegalArgumentException("Project not found: " + projectId));

        ProjectMapping mapping = project.getMapping();
        if (mapping == null || mapping.getJsonDocumentModel() == null) {
            throw new IllegalArgumentException("Project has no JSON document mapping configured.");
        }

        JsonDocumentModel documentModel = mapping.getJsonDocumentModel();
        if (documentModel.getRoot() == null) {
            throw new IllegalArgumentException("Project JSON mapping has no root object defined.");
        }

        return jsonSchemaBuilder.build(documentModel);
    }
}
