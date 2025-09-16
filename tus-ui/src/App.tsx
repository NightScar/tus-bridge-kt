/** biome-ignore-all lint/nursery/useUniqueElementIds: it's fine */
import Uppy from "@uppy/core";
import {
  Dropzone,
  FilesGrid,
  FilesList,
  UploadButton,
  UppyContextProvider,
} from "@uppy/react";
import Tus from "@uppy/tus";
import { useRef, useState } from "react";
import CustomDropzone from "./CustomDropzone";

import "./app.css";
import "@uppy/react/css/style.css";

function App() {
  const [uppy] = useState(() =>
    new Uppy().use(Tus, {
      endpoint: "https://tusd.tusdemo.net/files/",
    })
  );

  const dialogRef = useRef<HTMLDialogElement>(null);
  const [modalPlugin, setModalPlugin] = useState<
    "webcam" | "dropbox" | "screen-capture" | null
  >(null);

  function openModal(plugin: "webcam" | "dropbox" | "screen-capture") {
    setModalPlugin(plugin);
    dialogRef.current?.showModal();
  }

  function closeModal() {
    setModalPlugin(null);
    dialogRef.current?.close();
  }

  return (
    <UppyContextProvider uppy={uppy}>
      <main className="p-5 max-w-xl mx-auto">
        <h1 className="text-4xl font-bold my-4">Welcome to React.</h1>

        <UploadButton />

        <dialog
          ref={dialogRef}
          className="backdrop:bg-gray-500/50 rounded-lg shadow-xl p-0 fixed inset-0 m-auto"
        >
          {(() => {
            switch (modalPlugin) {
              default:
                return null;
            }
          })()}
        </dialog>

        <article>
          <h2 className="text-2xl my-4">With list</h2>
          <Dropzone />
          <FilesList />
        </article>

        <article>
          <h2 className="text-2xl my-4">With grid</h2>
          <Dropzone />
          <FilesGrid columns={2} />
        </article>

        <article>
          <h2 className="text-2xl my-4">With custom dropzone</h2>
          <CustomDropzone openModal={(plugin) => openModal(plugin)} />
        </article>
      </main>
    </UppyContextProvider>
  );
}

export default App;
