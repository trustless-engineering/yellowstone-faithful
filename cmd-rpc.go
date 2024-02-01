package main

import (
	"context"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"

	"github.com/allegro/bigcache/v3"
	"github.com/davecgh/go-spew/spew"
	"github.com/fsnotify/fsnotify"
	hugecache "github.com/rpcpool/yellowstone-faithful/huge-cache"
	"github.com/ryanuber/go-glob"
	"github.com/urfave/cli/v2"
	"golang.org/x/sync/errgroup"
	"k8s.io/klog/v2"
)

func newCmd_rpc() *cli.Command {
	var listenOn string
	var gsfaOnlySignatures bool
	var includePatterns cli.StringSlice
	var excludePatterns cli.StringSlice
	var watch bool
	var pathForProxyForUnknownRpcMethods string
	var epochSearchConcurrency int
	var epochLoadConcurrency int
	var maxCacheSizeMB int
	return &cli.Command{
		Name:        "rpc",
		Usage:       "Start a Solana JSON RPC server.",
		Description: "Provide multiple epoch config files, and start a Solana JSON RPC that exposes getTransaction, getBlock, and (optionally) getSignaturesForAddress",
		ArgsUsage:   "<one or more config files or directories containing config files (nested is fine)>",
		Before: func(c *cli.Context) error {
			return nil
		},
		Flags: append(lassieFetchFlags,
			&cli.StringFlag{
				Name:        "listen",
				Usage:       "Listen address",
				Value:       ":8899",
				Destination: &listenOn,
			},
			&cli.BoolFlag{
				Name:        "gsfa-only-signatures",
				Usage:       "gSFA: only return signatures",
				Value:       false,
				Destination: &gsfaOnlySignatures,
			},
			&cli.BoolFlag{
				Name:        "debug",
				Usage:       "Enable debug logging",
				Value:       false,
				Destination: &DebugMode,
			},
			&cli.StringSliceFlag{
				Name:        "include",
				Usage:       "Include files or dirs matching the given glob patterns",
				Value:       cli.NewStringSlice(),
				Destination: &includePatterns,
			},
			&cli.StringSliceFlag{
				Name:        "exclude",
				Usage:       "Exclude files or dirs matching the given glob patterns",
				Value:       cli.NewStringSlice(".git"),
				Destination: &excludePatterns,
			},
			&cli.BoolFlag{
				Name:        "watch",
				Usage:       "Watch the config files and directories for changes, and live-(re)load them",
				Value:       false,
				Destination: &watch,
			},
			&cli.StringFlag{
				Name:        "proxy",
				Usage:       "Path to a config file that will be used to proxy unknown RPC methods",
				Value:       "",
				Destination: &pathForProxyForUnknownRpcMethods,
			},
			&cli.IntFlag{
				Name:        "epoch-search-concurrency",
				Usage:       "How many epochs to search in parallel when searching for a signature",
				Value:       runtime.NumCPU(),
				Destination: &epochSearchConcurrency,
			},
			&cli.IntFlag{
				Name:        "epoch-load-concurrency",
				Usage:       "How many epochs to load in parallel when starting the RPC server",
				Value:       runtime.NumCPU(),
				Destination: &epochLoadConcurrency,
			},
			&cli.IntFlag{
				Name:        "max-cache",
				Usage:       "Maximum size of the cache in MB",
				Value:       0,
				Destination: &maxCacheSizeMB,
			},
		),
		Action: func(c *cli.Context) error {
			src := c.Args().Slice()
			configFiles, err := GetListOfConfigFiles(
				src,
				includePatterns.Value(),
				excludePatterns.Value(),
			)
			if err != nil {
				return cli.Exit(err.Error(), 1)
			}
			klog.Infof("Found %d config files:", len(configFiles))
			for _, configFile := range configFiles {
				fmt.Printf("  - %s\n", configFile)
			}

			conf := bigcache.DefaultConfig(5 * time.Minute)
			conf.HardMaxCacheSize = maxCacheSizeMB
			allCache, err := hugecache.NewWithConfig(c.Context, conf)
			if err != nil {
				return fmt.Errorf("failed to create cache: %w", err)
			}

			// Load configs:
			configs := make(ConfigSlice, 0)
			for _, configFile := range configFiles {
				config, err := LoadConfig(configFile)
				if err != nil {
					return cli.Exit(fmt.Sprintf("failed to load config file %q: %s", configFile, err.Error()), 1)
				}
				configs = append(configs, config)
			}
			// Validate configs:
			if err := configs.Validate(); err != nil {
				return cli.Exit(fmt.Sprintf("error validating configs: %s", err.Error()), 1)
			}
			configs.SortByEpoch()
			klog.Infof("Loaded %d epoch configs", len(configs))
			klog.Info("Initializing epochs...")

			epochs := make([]*Epoch, 0)
			wg := new(errgroup.Group)
			wg.SetLimit(epochLoadConcurrency)
			mu := &sync.Mutex{}
			for confIndex := range configs {
				config := configs[confIndex]
				wg.Go(func() error {
					epoch, err := NewEpochFromConfig(config, c, allCache)
					if err != nil {
						return fmt.Errorf("failed to create epoch from config %q: %s", config.ConfigFilepath(), err.Error())
					}
					mu.Lock()
					defer mu.Unlock()
					epochs = append(epochs, epoch)
					return nil
				})
			}
			if err := wg.Wait(); err != nil {
				return cli.Exit(fmt.Sprintf("failed to initialize epochs: %s", err.Error()), 1)
			}
			// Sort epochs by epoch number:
			sort.Slice(epochs, func(i, j int) bool {
				return epochs[i].Epoch() < epochs[j].Epoch()
			})

			multi := NewMultiEpoch(&Options{
				GsfaOnlySignatures:     gsfaOnlySignatures,
				EpochSearchConcurrency: epochSearchConcurrency,
			})

			defer func() {
				if err := multi.Close(); err != nil {
					klog.Errorf("error closing multi-epoch: %s", err.Error())
				}
			}()

			for _, epoch := range epochs {
				if err := multi.AddEpoch(epoch.Epoch(), epoch); err != nil {
					return cli.Exit(fmt.Sprintf("failed to add epoch %d: %s", epoch.Epoch(), err.Error()), 1)
				}
			}

			if watch {
				dirs, err := GetListOfDirectories(
					src,
					includePatterns.Value(),
					excludePatterns.Value(),
				)
				if err != nil {
					return cli.Exit(err.Error(), 1)
				}
				klog.Infof("Found %d directories; will start watching them for changes ...", len(dirs))
				spew.Dump(dirs)

				ctx, cancel := context.WithCancel(c.Context)
				defer cancel()

				// create a map that tracks files that are already being processed because of an event:
				// this is to avoid processing the same file multiple times
				// (e.g. if a file is create and then modified, we don't want to process it twice)
				fileProcessingTracker := make(map[string]struct{})
				mu := &sync.Mutex{}

				err = onFileChanged(ctx, dirs, func(event fsnotify.Event) {
					if !isJSONFile(event.Name) && !isYAMLFile(event.Name) {
						klog.Infof("File %q is not a JSON or YAML file; do nothing", event.Name)
						return
					}
					klog.Infof("File event: name=%q, op=%q", event.Name, event.Op)

					if event.Op != fsnotify.Remove && multi.HasEpochWithSameHashAsFile(event.Name) {
						klog.Infof("Epoch with same hash as file %q is already loaded; do nothing", event.Name)
						return
					}
					// register the file as being processed
					mu.Lock()
					_, ok := fileProcessingTracker[event.Name]
					if ok {
						klog.Infof("File %q is already being processed; do nothing", event.Name)
						mu.Unlock()
						return
					}
					fileProcessingTracker[event.Name] = struct{}{}
					mu.Unlock()
					// remove the file from the tracker when we're done processing it
					defer func() {
						mu.Lock()
						delete(fileProcessingTracker, event.Name)
						mu.Unlock()
					}()

					switch event.Op {
					case fsnotify.Write:
						{
							startedAt := time.Now()
							klog.Infof("File %q was modified; processing...", event.Name)
							// find the config file, load it, and update the epoch (replace)
							config, err := LoadConfig(event.Name)
							if err != nil {
								klog.Errorf("error loading config file %q: %s", event.Name, err.Error())
								return
							}
							epoch, err := NewEpochFromConfig(config, c, allCache)
							if err != nil {
								klog.Errorf("error creating epoch from config file %q: %s", event.Name, err.Error())
								return
							}
							err = multi.ReplaceOrAddEpoch(epoch.Epoch(), epoch)
							if err != nil {
								klog.Errorf("error replacing epoch %d: %s", epoch.Epoch(), err.Error())
								return
							}
							klog.Infof("Epoch %d added/replaced in %s", epoch.Epoch(), time.Since(startedAt))
						}
					case fsnotify.Create:
						{
							startedAt := time.Now()
							klog.Infof("File %q was created; processing...", event.Name)
							// find the config file, load it, and add it to the multi-epoch (if not already added)
							config, err := LoadConfig(event.Name)
							if err != nil {
								klog.Errorf("error loading config file %q: %s", event.Name, err.Error())
								return
							}
							epoch, err := NewEpochFromConfig(config, c, allCache)
							if err != nil {
								klog.Errorf("error creating epoch from config file %q: %s", event.Name, err.Error())
								return
							}
							err = multi.AddEpoch(epoch.Epoch(), epoch)
							if err != nil {
								klog.Errorf("error adding epoch %d: %s", epoch.Epoch(), err.Error())
								return
							}
							klog.Infof("Epoch %d added in %s", epoch.Epoch(), time.Since(startedAt))
						}
					case fsnotify.Remove:
						{
							startedAt := time.Now()
							klog.Infof("File %q was removed; processing...", event.Name)
							// find the epoch that corresponds to this file, and remove it (if any)
							epNumber, err := multi.RemoveEpochByConfigFilepath(event.Name)
							if err != nil {
								klog.Errorf("error removing epoch for config file %q: %s", event.Name, err.Error())
							}
							klog.Infof("Epoch %d removed in %s", epNumber, time.Since(startedAt))
						}
					case fsnotify.Rename:
						klog.Infof("File %q was renamed; do nothing", event.Name)
					case fsnotify.Chmod:
						klog.Infof("File %q had its permissions changed; do nothing", event.Name)
					default:
						klog.Infof("File %q had an unknown event %q; do nothing", event.Name, event.Op)
					}
				})
				if err != nil {
					return cli.Exit(err.Error(), 1)
				}
			}

			var listenerConfig *ListenerConfig
			if pathForProxyForUnknownRpcMethods != "" {
				proxyConfig, err := LoadProxyConfig(pathForProxyForUnknownRpcMethods)
				if err != nil {
					return cli.Exit(fmt.Sprintf("failed to load proxy config file %q: %s", pathForProxyForUnknownRpcMethods, err.Error()), 1)
				}
				listenerConfig = &ListenerConfig{
					ProxyConfig: proxyConfig,
				}
			}

			return multi.ListenAndServe(c.Context, listenOn, listenerConfig)
		},
	}
}

// TODO:
// - [ ] get the list of provided arguments, and distinguish between files and directories
// - [ ] load all the config files, etc.
// - [ ] start a goroutine that monitors the config files for changes
// - [ ] when a config file changes, reload it and update the epoch
// - [ ] start a goroutine that monitors the directories and subdirectories for changes (new files, deleted files, etc.)
// - is only watching directories sufficient? or do we need to watch files too?
func onFileChanged(ctx context.Context, dirs []string, callback func(fsnotify.Event)) error {
	// monitor a directory for file changes
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return fmt.Errorf("failed to create watcher: %w", err)
	}

	// start watching the directories
	for _, path := range dirs {
		err = watcher.Add(path)
		if err != nil {
			return fmt.Errorf("failed to add path %q to watcher: %w", path, err)
		}
	}

	// start a goroutine to handle events
	go func() {
		defer watcher.Close()
		for {
			select {
			case <-ctx.Done():
				return
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				callback(event)
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				klog.Errorf("error watching files: %v", err)
			}
		}
	}()

	return nil
}

// GetListOfDirectories returns a list of all the directories in the given directories and subdirectories
// that match one of the given patterns.
// The directories are first matched against the include patterns, and then against the exclude patterns.
// If no include patterns are provided, then all directories are included.
// If no exclude patterns are provided, then no directories are excluded.
// The `.git` directory is always excluded.
func GetListOfDirectories(src []string, includePatterns []string, excludePatterns []string) ([]string, error) {
	var allDirs []string

	for _, srcItem := range src {
		isDir, err := isDirectory(srcItem)
		if err != nil {
			return nil, err
		}
		if isDir {
			dirs, err := getDeepDirectories(srcItem, includePatterns, excludePatterns)
			if err != nil {
				return nil, err
			}
			allDirs = append(allDirs, dirs...)
		} else {
			if matchesWithIncludeExcludePatterns(srcItem, includePatterns, excludePatterns) {
				allDirs = append(allDirs, srcItem)
			}
		}
	}

	deduped := deduplicate(allDirs)
	return deduped, nil
}

func matchesWithIncludeExcludePatterns(item string, includePatterns []string, excludePatterns []string) bool {
	if len(includePatterns) == 0 && len(excludePatterns) == 0 {
		return true
	}
	if len(includePatterns) > 0 {
		_, ok := hasMatch(item, includePatterns)
		if !ok {
			return false
		}
	}
	if len(excludePatterns) > 0 {
		_, ok := hasMatch(item, excludePatterns)
		if ok {
			return false
		}
	}
	return true
}

func getDeepDirectories(dir string, includePatterns []string, excludePatterns []string) ([]string, error) {
	ok, err := exists(dir)
	if err != nil {
		return nil, fmt.Errorf("error checking if path %q exists: %w", dir, err)
	}
	if !ok {
		return nil, fmt.Errorf("path %q does not exist", dir)
	}

	isDir, err := isDirectory(dir)
	if err != nil {
		return nil, fmt.Errorf("error checking if path %q is a directory: %w", dir, err)
	}
	if !isDir {
		return nil, fmt.Errorf("path %q is not a directory", dir)
	}

	dirs, err := walkDirectoryMatchingSubdirectories(dir, includePatterns, excludePatterns)
	if err != nil {
		return nil, fmt.Errorf("error walking directory %q: %w", dir, err)
	}

	return dirs, nil
}

// GetListOfConfigFiles returns a list of all the config files in the given directories and subdirectories
// that match one of the given patterns.
// The files are first matched against the file extension patterns, then against the include patterns,
// and finally against the exclude patterns.
func GetListOfConfigFiles(src []string, includePatterns []string, excludePatterns []string) ([]string, error) {
	fileExtensionPatterns := []string{"*.yaml", "*.yml", "*.json"}

	var allFiles []string

	for _, srcItem := range src {
		isDir, err := isDirectory(srcItem)
		if err != nil {
			return nil, err
		}
		if isDir {
			files, err := getDeepFilesFromDirectory(srcItem, func(entry string) bool {
				return itemMatchesAnyPattern(entry, fileExtensionPatterns...) && matchesWithIncludeExcludePatterns(entry, includePatterns, excludePatterns)
			})
			if err != nil {
				return nil, err
			}
			allFiles = append(allFiles, files...)
		} else {
			if itemMatchesAnyPattern(srcItem, fileExtensionPatterns...) && matchesWithIncludeExcludePatterns(srcItem, includePatterns, excludePatterns) {
				allFiles = append(allFiles, srcItem)
			}
		}
	}

	return deduplicate(allFiles), nil
}

// getDeepFilesFromDirectory returns a list of all the files in the given directory and its subdirectories
// that match one of the given patterns.
func getDeepFilesFromDirectory(dir string, filter func(string) bool) ([]string, error) {
	ok, err := exists(dir)
	if err != nil {
		return nil, fmt.Errorf("error checking if path %q exists: %w", dir, err)
	}
	if !ok {
		return nil, fmt.Errorf("path %q does not exist", dir)
	}

	isDir, err := isDirectory(dir)
	if err != nil {
		return nil, fmt.Errorf("error checking if path %q is a directory: %w", dir, err)
	}
	if !isDir {
		return nil, fmt.Errorf("path %q is not a directory", dir)
	}

	files, err := walkDirectoryMatchingFiles(dir, filter)
	if err != nil {
		return nil, fmt.Errorf("error walking directory %q: %w", dir, err)
	}

	return files, nil
}

// wallk a given directory and return a list of all the files that match the given patterns
func walkDirectoryMatchingFiles(dir string, filter func(string) bool) ([]string, error) {
	var matching []string

	err := fs.WalkDir(os.DirFS(dir), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			klog.Errorf("error walking path %q: %v", path, err)
			return err
		}
		if d.IsDir() {
			return nil
		}
		path, err = filepath.Abs(filepath.Join(dir, path))
		if err != nil {
			return err
		}
		matches := filter(path)
		if matches {
			matching = append(matching, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error walking directory %q: %w", dir, err)
	}

	return matching, nil
}

func walkDirectoryMatchingSubdirectories(dir string, includePatterns []string, excludePatterns []string) ([]string, error) {
	var matching []string

	err := fs.WalkDir(os.DirFS(dir), ".", func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			klog.Errorf("error walking path %q: %v", path, err)
			return err
		}
		if !d.IsDir() {
			return nil
		}
		path, err = filepath.Abs(filepath.Join(dir, path))
		if err != nil {
			return err
		}
		{
			// if matches `.git` then exclude it
			if d.IsDir() && (d.Name() == ".git") {
				return filepath.SkipDir
			}
		}
		matches := matchesWithIncludeExcludePatterns(path, includePatterns, excludePatterns)
		if matches {
			matching = append(matching, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("error walking directory %q: %w", dir, err)
	}

	return matching, nil
}

func selectMatching(items []string, patterns ...string) []string {
	var matching []string
	for _, item := range items {
		matches := itemMatchesAnyPattern(item, patterns...)
		if matches {
			matching = append(matching, item)
		}
	}
	return matching
}

func selectNotMatching(items []string, patterns ...string) []string {
	var matching []string
	for _, item := range items {
		matches := itemMatchesAnyPattern(item, patterns...)
		if !matches {
			matching = append(matching, item)
		}
	}
	return matching
}

func itemMatchesAnyPattern(item string, patterns ...string) bool {
	_, ok := hasMatch(item, patterns)
	return ok
}

// hasMatch finds the matching pattern (glob) to which the provided item matches.
func hasMatch(item string, patterns []string) (string, bool) {
	if item == "" {
		return "", false
	}

	// sort the patterns in increasing length order:
	sort.Strings(patterns)

	// first, try to find a precise match:
	for _, pattern := range patterns {
		if pattern == item {
			return pattern, true
		}
	}
	// ... then look for a glob match:
	for _, pattern := range patterns {
		if isMatch := glob.Glob(pattern, item); isMatch {
			return pattern, true
		}
	}
	return "", false
}

func deduplicate(items []string) []string {
	seen := make(map[string]struct{})
	var deduped []string
	for _, item := range items {
		if _, ok := seen[item]; !ok {
			seen[item] = struct{}{}
			deduped = append(deduped, item)
		}
	}
	return deduped
}