package generator

import (
	"context"
	"io/fs"
	"os"
	"path/filepath"
	"strings"

	"github.com/pkg/errors"
	"github.com/sirupsen/logrus"

	"github.com/dragonflyoss/image-service/contrib/nydusify/pkg/build"
	"github.com/dragonflyoss/image-service/contrib/nydusify/pkg/parser"
	"github.com/dragonflyoss/image-service/contrib/nydusify/pkg/provider"
	"github.com/dragonflyoss/image-service/contrib/nydusify/pkg/utils"
)

// Opt defines Chunkdict generate options.
// Note: sources is one or more Nydus image references.
type Opt struct {
	WorkDir        string
	Sources        []string
	SourceInsecure bool
	NydusImagePath string
	ExpectedArch   string
}

// Generator generates chunkdict by deduplicating multiple nydus images
// invoking "nydus-image chunkdict save" to save image information into database.
type Generator struct {
	Opt
	sourcesParser []*parser.Parser
}

// New creates Generator instance.
func New(opt Opt) (*Generator, error) {
	// TODO: support sources image resolver
	var sourcesParser []*parser.Parser
	for _, source := range opt.Sources {
		sourcesRemote, err := provider.DefaultRemote(source, opt.SourceInsecure)
		if err != nil {
			return nil, errors.Wrap(err, "Init source image parser")
		}
		sourceParser, err := parser.New(sourcesRemote, opt.ExpectedArch)
		sourcesParser = append(sourcesParser, sourceParser)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create parser")
		}
	}

	generator := &Generator{
		Opt:           opt,
		sourcesParser: sourcesParser,
	}

	return generator, nil
}

// Generate saves multiple Nydus bootstraps into the database one by one.
func (generator *Generator) Generate(ctx context.Context) error {
	var bootstrapSlice []string
	if err := generator.pull(ctx, &bootstrapSlice); err != nil {
		if utils.RetryWithHTTP(err) {
			for index := range generator.Sources {
				generator.sourcesParser[index].Remote.MaybeWithHTTP(err)
			}
		}
		if err := generator.pull(ctx, &bootstrapSlice); err != nil {
			return err
		}
	}

	if err := generator.generate(ctx, bootstrapSlice); err != nil {
		return err
	}
	return nil
}

func (generator *Generator) pull(ctx context.Context, bootstrapSlice *[]string) error {
	for index := range generator.Sources {
		sourceParsed, err := generator.sourcesParser[index].Parse(ctx)
		if err != nil {
			return errors.Wrap(err, "parse Nydus image")
		}

		// Create a directory to store the image bootstrap
		nydusImageName := strings.Replace(generator.Sources[index], "/", ":", -1)
		bootstrapFolderPath := filepath.Join(generator.WorkDir, nydusImageName)
		if err := os.MkdirAll(bootstrapFolderPath, fs.ModePerm); err != nil {
			return errors.Wrap(err, "creat work directory")
		}
		if err := generator.Output(ctx, sourceParsed, bootstrapFolderPath, index); err != nil {
			return errors.Wrap(err, "output image information")
		}
		bootstrapPath := filepath.Join(bootstrapFolderPath, "nydus_bootstrap")
		*bootstrapSlice = append(*bootstrapSlice, bootstrapPath)
	}
	return nil
}

func (generator *Generator) generate(ctx context.Context, bootstrapSlice []string) error {
	// Invoke "nydus-image generate" command
	builder := build.NewBuilder(generator.NydusImagePath)
	databaseType := "sqlite"
	if err := builder.Generate(build.GenerateOption{
		BootstrapSlice:     bootstrapSlice,
		MergeBootstrapPath: filepath.Join(generator.WorkDir, "merge_bootstrap"),
		DatabasePath:       databaseType + "://" + filepath.Join(generator.WorkDir, "database.db"),
		OutputPath:         filepath.Join(generator.WorkDir, "nydus_bootstrap_output.json"),
	}); err != nil {
		return errors.Wrap(err, "invalid nydus bootstrap format")
	}

	logrus.Infof("Successfully generate image chunk dictionary")

	return nil
}
