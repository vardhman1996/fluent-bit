const React = require('react');

const CompLibrary = require('../../core/CompLibrary');
const MarkdownBlock = CompLibrary.MarkdownBlock; /* Used to read markdown */
const Container = CompLibrary.Container;
const GridBlock = CompLibrary.GridBlock;

const CWD = process.cwd();

const translate = require('../../server/translate.js').translate;

const siteConfig = require(`${CWD}/siteConfig.js`);
const releases = require(`${CWD}/releases.json`);

function getLatestArchiveMirrorUrl(version, type) {
    return `https://www.apache.org/dyn/mirrors/mirrors.cgi?action=download&filename=pulsar/pulsar-${version}/apache-pulsar-${version}-${type}.tar.gz`
}

function distUrl(version, type) {
    return `https://www.apache.org/dist/pulsar/pulsar-${version}/apache-pulsar-${version}-${type}.tar.gz`
}

function archiveUrl(version, type) {
    return `https://archive.apache.org/dist/pulsar/pulsar-${version}/apache-pulsar-${version}-${type}.tar.gz`
}

class Download extends React.Component {
  render() {
    const latestVersion = releases[0];
    const latestArchiveMirrorUrl = getLatestArchiveMirrorUrl(latestVersion, 'bin');
    const latestSrcArchiveMirrorUrl = getLatestArchiveMirrorUrl(latestVersion, 'src');
    const latestArchiveUrl = distUrl(latestVersion, 'bin');
    const latestSrcArchiveUrl = distUrl(latestVersion, 'src')

    const releaseInfo = releases.map(version => {
      return {
        version: version,
        binArchiveUrl: archiveUrl(version, 'bin'),
        srcArchiveUrl: archiveUrl(version, 'src')
      }
    });

    return (
      <div className="pageContainer">
        <Container className="mainContainer documentContainer postContainer">
          <div className="post">
            <header className="postHeader">
              <h1><translate>Apache Pulsar downloads</translate></h1>
              <hr />
            </header>
            <h2 id="latest"><translate>Current version (Stable)</translate> {latestVersion}</h2>
            <table className="versions" style={{width:'100%'}}>
              <thead>
                <tr>
                  <th><translate>Release</translate></th>
                  <th><translate>Link</translate></th>
                  <th><translate>Crypto files</translate></th>
                </tr>
              </thead>
              <tbody>
                <tr key={'binary'}>
                  <th><translate>Binary</translate></th>
                  <td>
                    <a href={latestArchiveMirrorUrl}>apache-pulsar-{latestVersion}-bin.tar.gz</a>
                  </td>
                  <td>
                    <a href={`${latestArchiveUrl}.asc`}>asc</a>,&nbsp;
                    <a href={`${latestArchiveUrl}.sha512`}>sha512</a>
                  </td>
                </tr>
                <tr key={'source'}>
                  <th><translate>Source</translate></th>
                  <td>
                    <a href={latestSrcArchiveMirrorUrl}>apache-pulsar-{latestVersion}-src.tar.gz</a>
                  </td>
                  <td>
                    <a href={`${latestSrcArchiveUrl}.asc`}>asc</a>,&nbsp;
                    <a href={`${latestSrcArchiveUrl}.sha512`}>sha512</a>
                  </td>
                </tr>
              </tbody>
            </table>

            <h2><translate>Release Integrity</translate></h2>
            <MarkdownBlock>
              You must [verify](https://www.apache.org/info/verification.html) the integrity of the downloaded files.
              We provide OpenPGP signatures for every release file. This signature should be matched against the
              [KEYS](https://www.apache.org/dist/incubator/pulsar/KEYS) file which contains the OpenPGP keys of
              Pulsar's Release Managers. We also provide `SHA-512` checksums for every release file.
              After you download the file, you should calculate a checksum for your download, and make sure it is
              the same as ours.
            </MarkdownBlock>


            <h2><translate>Release notes</translate></h2>
            <div>
              <p>
                <a href={`${siteConfig.baseUrl}${this.props.language}/release-notes`}>Release notes</a> for all Pulsar's versions
              </p>
            </div>

            <h2><translate>Getting started</translate></h2>
            <div>
              <p>
              <translate>
                Once you've downloaded a Pulsar release, instructions on getting up and running with a standalone cluster
                that you can run on your laptop can be found in the{' '}
              </translate>&nbsp;
                <a href={`${siteConfig.baseUrl}docs/${this.props.language}/standalone`}><translate>Run Pulsar locally</translate></a> <translate>tutorial</translate>.
              </p>
            </div>
            <p>
              <translate>
              If you need to connect to an existing Pulsar cluster or instance using an officially supported client,
              see the client docs for these languages:
              </translate>
            </p>
            <table className="clients">
              <thead>
                <tr>
                  <th><translate>Client guide</translate></th>
                  <th><translate>API docs</translate></th>
                </tr>
              </thead>
              <tbody>
                <tr key={'java'}>
                  <td><a href={`${siteConfig.baseUrl}docs/${this.props.language}/client-libraries-java`}><translate>The Pulsar java client</translate></a></td>
                  <td><translate>The Pulsar java client</translate></td>
                </tr>
                <tr key={'go'}>
                  <td><a href={`${siteConfig.baseUrl}docs/${this.props.language}/client-libraries-go`}><translate>The Pulsar go client</translate></a></td>
                  <td><translate>The Pulsar go client</translate></td>
                </tr>
                <tr key={'python'}>
                  <td><a href={`${siteConfig.baseUrl}docs/${this.props.language}/client-libraries-python`}><translate>The Pulsar python client</translate></a></td>
                  <td><translate>The Pulsar python client</translate></td>
                </tr>
                <tr key={'cpp'}>
                  <td><a href={`${siteConfig.baseUrl}docs/${this.props.language}/client-libraries-cpp`}><translate>The Pulsar C++ client</translate></a></td>
                  <td><translate>The Pulsar C++ client</translate></td>
                </tr>
              </tbody>
            </table>

            <h2 id="archive"><translate>Older releases</translate></h2>
            <table className="versions">
              <thead>
                <tr>
                  <th><translate>Release</translate></th>
                  <th><translate>Binary</translate></th>
                  <th><translate>Source</translate></th>
                  <th><translate>Release notes</translate></th>
                </tr>
              </thead>
              <tbody>
                {releaseInfo.map(
                  info =>
                    info.version !== latestVersion && (
                      <tr key={info.version}>
                        <th>{info.version}</th>
                        <td>
                          <a href={info.binArchiveUrl}>apache-pulsar-{info.version}-bin-tar.gz</a>
                          &nbsp;
                          (<a href={`${info.binArchiveUrl}.asc`}>asc</a>,&nbsp;
                            <a href={`${info.binArchiveUrl}.sha512`}>sha512</a>)
                        </td>
                        <td>
                          <a href={info.srcArchiveUrl}>apache-pulsar-{info.version}-bin-tar.gz</a>
                          &nbsp;
                          (<a href={`${info.srcArchiveUrl}.asc`}>asc</a>&nbsp;
                            <a href={`${info.srcArchiveUrl}.sha512`}>sha512</a>)
                        </td>
                        <td>
                          <a href={`${siteConfig.baseUrl}${this.props.language}/release-notes#${info.version}`}><translate>Release Notes</translate></a>
                        </td>
                      </tr>
                    )
                )}
              </tbody>
            </table>
          </div>
        </Container>
      </div>
    );
  }
}

module.exports = Download;
